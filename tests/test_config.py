import tempfile
import unittest
from pathlib import Path

from omniqueue.config import ClusterConfig, Config, ConfigError, config_from_dict


class LogoResolutionTests(unittest.TestCase):
    def test_sources(self):
        with tempfile.TemporaryDirectory() as tmp:
            logos = Path(tmp) / "logos"
            logos.mkdir()
            (logos / "a.png").write_bytes(b"png")
            explicit = Path(tmp) / "custom.svg"
            explicit.write_text("<svg/>")
            cfg = Config(
                clusters=[
                    ClusterConfig(name="a", host="a"),
                    ClusterConfig(name="b", host="b", logo=str(explicit)),
                    ClusterConfig(name="c", host="c", logo="https://example.org/c.png"),
                    ClusterConfig(name="d", host="d"),
                    ClusterConfig(name="e", host="e", logo=str(Path(tmp) / "missing.png")),
                ],
                logo_dir=logos,
            )
            by = {c.name: c for c in cfg.clusters}
            self.assertEqual(cfg.logo_source(by["a"]), str(logos / "a.png"))
            self.assertEqual(cfg.logo_source(by["b"]), str(explicit))
            self.assertEqual(cfg.logo_source(by["c"]), "https://example.org/c.png")
            self.assertIsNone(cfg.logo_source(by["d"]))
            self.assertIsNone(cfg.logo_source(by["e"]))


class ConfigParsingTests(unittest.TestCase):
    def test_minimal(self):
        cfg = config_from_dict({"clusters": [{"name": "x", "host": "x.example", "logo": "~/x.png"}]})
        self.assertEqual(cfg.clusters[0].logo, "~/x.png")
        self.assertTrue(cfg.persist_connections)

    def test_errors(self):
        with self.assertRaises(ConfigError):
            config_from_dict({})
        with self.assertRaises(ConfigError):
            config_from_dict({"clusters": [{"host": "no-name"}]})
        with self.assertRaises(ConfigError):
            config_from_dict({"clusters": [{"name": "x", "bogus": 1}]})
        with self.assertRaises(ConfigError):
            config_from_dict({"clusters": [{"name": "x"}, {"name": "x"}]})
        with self.assertRaises(ConfigError):
            config_from_dict({"clusters": [{"name": "x"}], "refresh_seconds": 1})


if __name__ == "__main__":
    unittest.main()
