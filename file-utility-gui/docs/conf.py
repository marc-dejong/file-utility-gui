# -- Project information -----------------------------------------------------
project = 'File Utility GUI'
author = 'Marc de Jong'
release = '2025-08-28-a'
copyright = '2025, Marc de Jong'

# -- Path setup --------------------------------------------------------------
import os, sys
sys.path.insert(0, os.path.abspath('..'))  # allow importing project modules

# -- General configuration ---------------------------------------------------
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.napoleon',   # Google/NumPy style docstrings
    'myst_parser',           # Markdown support
]
autosummary_generate = True
templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']
source_suffix = {'.rst': 'restructuredtext', '.md': 'markdown'}

# Helpful defaults / mocks for imports that aren't available at build time
autodoc_typehints = 'description'
napoleon_google_docstring = True
napoleon_numpy_docstring = True
autodoc_mock_imports = ['normalizer_12x', 'tkinter', 'pandas', 'dask']

# -- HTML --------------------------------------------------------------------
html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']

# -- Footer / Theme options -----------------------------------------------
html_theme_options = {
    # Show version and release info in the footer
    "display_version": True,
}

# Use your defined 'release' string from above (already set to 2025-08-28-a)
html_show_sphinx = False   # (optional: hides "Built with Sphinx" text)

