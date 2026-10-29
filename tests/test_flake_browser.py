"""Tests that the overlord image carries a headless browser (DR-13).

doc-review's deck fit check measures real layout by running deck_dom.js
checkFit() in a browser, so the bot needs one inside the container. pip's
Playwright cannot supply it: its bundled node binary does not run on Nix.
Chaos approved adding Chromium to the image on 2026-10-29.

The image has no /etc/fonts, so fontconfig finds no fonts unless
FONTCONFIG_FILE names a config. Without one, any text that falls back past
doc-review's bundled web fonts renders as tofu and measures wrong.

These read flake.nix as text, the way scripts/test_flake_docs.py does, since
there is no nix binary in the container. The first real build catches a
mistyped attribute.
"""

import re
import unittest
from pathlib import Path

FLAKE = Path(__file__).resolve().parent.parent / "flake.nix"


def _block(text, start_pattern):
    """Return the bracketed list that follows start_pattern, without comments."""
    match = re.search(start_pattern + r"\s*\[", text)
    if not match:
        raise AssertionError(f"{start_pattern!r} not found in flake.nix")
    depth, i = 1, match.end()
    while depth:
        depth += {"[": 1, "]": -1}.get(text[i], 0)
        i += 1
    body = text[match.end():i - 1]
    return re.sub(r"#[^\n]*", "", body)


class TestImageBrowser(unittest.TestCase):
    def setUp(self):
        self.text = FLAKE.read_text()

    def test_runtime_packages_include_chromium(self):
        packages = _block(self.text, r"runtimePackages\s*=")
        self.assertRegex(packages, r"\bpkgs\.chromium\b")

    def test_env_points_fontconfig_at_a_font_config(self):
        env = _block(self.text, r"Env\s*=")
        self.assertRegex(env, r'"FONTCONFIG_FILE=\$\{fontsConf\}"')

    def test_fonts_conf_lists_a_font_directory(self):
        match = re.search(
            r"fontsConf\s*=\s*pkgs\.makeFontsConf\s*\{(.*?)\};", self.text, re.S
        )
        self.assertIsNotNone(match, "fontsConf = pkgs.makeFontsConf { ... }; missing")
        self.assertRegex(match.group(1), r"fontDirectories\s*=\s*\[\s*pkgs\.\w+")

    def test_fonts_conf_includes_fontconfig_conf_d(self):
        # makeFontsConf's default include is /etc/fonts/conf.d, which the image
        # lacks, so sans-serif and monospace would map to no font in particular.
        match = re.search(
            r"fontsConf\s*=\s*pkgs\.makeFontsConf\s*\{(.*?)\};", self.text, re.S
        )
        self.assertIsNotNone(match, "fontsConf = pkgs.makeFontsConf { ... }; missing")
        self.assertRegex(
            match.group(1),
            r'includes\s*=\s*\[\s*"\$\{pkgs\.fontconfig\.out\}/etc/fonts/conf\.d"',
        )


if __name__ == "__main__":
    unittest.main()
