"""Tests for BlackRoad Load Balancer."""
import pytest
from load_balancer import BackendServer, LoadBalancer, _init_db


def make_server(**kwargs):
    defaults = dict(host="127.0.0.1", port=8080)
    defaults.update(kwargs)
    return BackendServer(**defaults)


def make_lb(tmp_path):
    return LoadBalancer(db_path=tmp_path / "test_lb.db")


class TestBackendServer:
    def test_basic_server(self):
        s = make_server()
        assert s.address == "127.0.0.1:8080"

    def test_invalid_port_zero(self):
        with pytest.raises(ValueError):
            make_server(port=0)

    def test_invalid_port_high(self):
        with pytest.raises(ValueError):
            make_server(port=65536)

    def test_invalid_weight(self):
        with pytest.raises(ValueError):
            make_server(weight=0)

    def test_empty_host(self):
        with pytest.raises(ValueError):
            make_server(host="")

    def test_avg_response_empty(self):
        s = make_server()
        assert s.avg_response_ms == 0.0

    def test_avg_response_computed(self):
        s = make_server()
        s.response_times = [100.0, 200.0, 300.0]
        assert s.avg_response_ms == 200.0

    def test_p95_response(self):
        s = make_server()
        s.response_times = list(range(1, 101))
        assert s.p95_response_ms >= 95

    def test_error_rate_zero(self):
        s = make_server()
        assert s.error_rate == 0.0

    def test_error_rate_computed(self):
        s = make_server(total_requests=10, failed_requests=2)
        assert s.error_rate == 0.2


class TestLoadBalancerAddRemove:
    def test_add_server(self, tmp_path):
        lb = make_lb(tmp_path)
        assert lb.add_server(make_server(port=8080)) is True
        assert len(lb._servers) == 1

    def test_add_duplicate(self, tmp_path):
        lb = make_lb(tmp_path)
        lb.add_server(make_server(port=8080))
        assert lb.add_server(make_server(port=8080)) is False
        assert len(lb._servers) == 1

    def test_remove_server(self, tmp_path):
        lb = make_lb(tmp_path)
        lb.add_server(make_server(port=8080))
        assert lb.remove_server("127.0.0.1", 8080) is True
        assert len(lb._servers) == 0

    def test_remove_nonexistent(self, tmp_path):
        lb = make_lb(tmp_path)
        assert lb.remove_server("127.0.0.1", 9999) is False


class TestRoundRobin:
    def test_basic_round_robin(self, tmp_path):
        lb = make_lb(tmp_path)
        lb.add_server(make_server(port=8080))
        lb.add_server(make_server(port=8081))
        lb.add_server(make_server(port=8082))
        ports = []
        for _ in range(6):
            s = lb.get_next_server("round_robin")
            lb.release_connection(s)
            ports.append(s.port)
        # Should visit each server twice
        assert ports.count(8080) == 2
        assert ports.count(8081) == 2
        assert ports.count(8082) == 2

    def test_no_healthy_servers(self, tmp_path):
        lb = make_lb(tmp_path)
        lb.add_server(make_server(port=8080, health=False))
        assert lb.get_next_server("round_robin") is None


class TestWeightedRoundRobin:
    def test_distribution_follows_weight(self, tmp_path):
        lb = make_lb(tmp_path)
        lb.add_server(make_server(port=8080, weight=1))
        lb.add_server(make_server(port=8081, weight=3))
        counts: dict[int, int] = {8080: 0, 8081: 0}
        for _ in range(40):
            s = lb.get_next_server("weighted_round_robin")
            lb.release_connection(s)
            counts[s.port] += 1
        assert counts[8081] > counts[8080]


class TestLeastConnections:
    def test_routes_to_least_loaded(self, tmp_path):
        lb = make_lb(tmp_path)
        lb.add_server(make_server(port=8080))
        lb.add_server(make_server(port=8081))
        lb.add_server(make_server(port=8082))
        # Load up 8080 and 8081
        lb._servers[0].active_conns = 10
        lb._servers[1].active_conns = 5
        lb._servers[2].active_conns = 1
        s = lb.get_next_server("least_connections")
        assert s.port == 8082


class TestIPHash:
    def test_consistent_for_same_ip(self, tmp_path):
        lb = make_lb(tmp_path)
        for p in [8080, 8081, 8082]:
            lb.add_server(make_server(port=p))
        results = set()
        for _ in range(10):
            s = lb.get_next_server("ip_hash", "192.168.1.100")
            lb.release_connection(s)
            results.add(s.port)
        assert len(results) == 1

    def test_different_ips_may_route_differently(self, tmp_path):
        lb = make_lb(tmp_path)
        for p in [8080, 8081, 8082]:
            lb.add_server(make_server(port=p))
        ports = set()
        for ip_suffix in range(20):
            s = lb.get_next_server("ip_hash", f"10.0.0.{ip_suffix}")
            lb.release_connection(s)
            ports.add(s.port)
        assert len(ports) > 1


class TestInvalidAlgorithm:
    def test_unknown_algorithm(self, tmp_path):
        lb = make_lb(tmp_path)
        lb.add_server(make_server())
        with pytest.raises(ValueError, match="Unknown algorithm"):
            lb.get_next_server("teleportation")


class TestStatsReport:
    def test_empty_stats(self, tmp_path):
        lb = make_lb(tmp_path)
        report = lb.stats_report()
        assert report["total_servers"] == 0
        assert report["total_requests"] == 0

    def test_stats_after_requests(self, tmp_path):
        lb = make_lb(tmp_path)
        lb.add_server(make_server(port=8080))
        for _ in range(5):
            s = lb.get_next_server("round_robin")
            lb.release_connection(s, success=True, latency_ms=50.0)
        report = lb.stats_report()
        assert report["total_servers"] == 1
        assert report["servers"][0]["total_requests"] == 5


class TestSimulate:
    def test_simulation_distributes_requests(self, tmp_path):
        lb = make_lb(tmp_path)
        for p in [8080, 8081, 8082]:
            lb.add_server(make_server(port=p))
        result = lb.simulate_requests(300, "round_robin")
        assert result["count"] == 300
        total = sum(result["distribution"].values())
        assert total == 300
