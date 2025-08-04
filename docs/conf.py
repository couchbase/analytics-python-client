# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))

import os
import sys

sys.path.insert(0, os.path.abspath('..'))
sys.path.insert(0, os.path.dirname(__file__))

# -- Project information -----------------------------------------------------

project = 'Couchbase Python Analytics Client Library'
copyright = '2025, Couchbase, Inc.'
author = 'Couchbase, Inc.'

# from .. import couchbase_version
import couchbase_analytics_version  # nopep8 # isort:skip # noqa: E402

try:
    from datetime import datetime
    year = f'{datetime.today():%Y}'
except BaseException:
    year = '2024'
copyright = f'2016-{year}, Couchbase, Inc.'

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# The full version, including alpha/beta/rc tags.
sdk_version = couchbase_analytics_version.get_version()
version = sdk_version
release = sdk_version


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx_rtd_theme',
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.extlinks',
    'sphinx_copybutton',
    'enum_tools.autoenum',
    # 'sphinx.ext.autodoc.typehints',
    # 'sphinx_toolbox.more_autodoc.overloads',
    'sphinx_autodoc_typehints',
    'sphinx.ext.intersphinx'
]

typehints_use_signature = True
autodoc_type_aliases = {'JSONType': 'couchbase_analytics.common.JSONType'}

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store', 'docs_mock']

intersphinx_mapping = {'python': ('https://docs.python.org/3', None)}

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'sphinx_rtd_theme'
# html_theme = 'classic'

html_theme_options = {
    'display_version': True,
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
# html_static_path = ['_static']
html_static_path = []

# The docs are unclear, but adding the `%s` to the end of the URL prevents:
#   TypeError: not all arguments converted during string formatting
extlinks = {
    'couchbase_dev_portal': ('https://developer.couchbase.com/%s', None),
    'couchbase_discord': ('https://discord.com/invite/sQ5qbPZuTh%s', None),
    'analytics_sdk_github': ('https://github.com/couchbaselabs/analytics-python-client%s', None),
    'acouchbase_analytics_examples':
        ('https://github.com/couchbaselabs/analytics-python-client/tree/main/async/examples%s', None),
    'couchbase_analytics_examples':
        ('https://github.com/couchbaselabs/analytics-python-client/tree/main/sync/examples%s', None),
    'analytics_sdk_jira': ('https://issues.couchbase.com/projects/PYCO/issues/%s', None),
    'analytics_sdk_docs': ('https://docs.couchbase.com/python-sdk/current/hello-world/overview.html%s', None),
    'analytics_sdk_release_notes':
        ('https://docs.couchbase.com/python-sdk/current/project-docs/sdk-release-notes.html%s', None),
    'analytics_sdk_compatibility':
        ('https://docs.couchbase.com/python-sdk/current/project-docs/compatibility.html%s', None),
    'analytics_sdk_forums': ('https://forums.couchbase.com/c/python-sdk/10%s', None),
    'analytics_sdk_license': ('https://github.com/couchbaselabs/analytics-python-client/blob/main/LICENSE%s', None),
    'analytics_sdk_contribute':
        ('https://github.com/couchbaselabs/analytics-python-client/blob/main/CONTRIBUTING.md%s', None),
    'analytics_sdk_version_compat':
        ('https://docs.couchbase.com/python-sdk/current/project-docs/compatibility.html#python-version-compat%s', None),
}
