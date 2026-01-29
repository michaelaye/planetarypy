"""Custom quartodoc renderer that adds GitHub source links to documentation.

This renderer extends quartodoc's MdRenderer to add [source] links that point
to the corresponding GitHub file and line numbers for functions and classes.

Usage in _quarto.yml:
    quartodoc:
      renderer:
        style: _renderer.py
        repo_url: https://github.com/michaelaye/planetarypy
        branch: main
        source_dir: src
"""

from __future__ import annotations

from typing import Union

from plum import dispatch
from quartodoc import layout
from quartodoc.renderers import MdRenderer


class SourceLinkMdRenderer(MdRenderer):
    """Markdown renderer with GitHub source links.

    Adds [source] links to function and class documentation that link
    to the corresponding file and line numbers on GitHub.

    Parameters
    ----------
    repo_url : str
        The base URL of the GitHub repository.
    branch : str
        The branch name to link to (default: "main").
    source_dir : str
        The directory containing source code (default: "src").
    **kwargs
        Additional arguments passed to MdRenderer.
    """

    style = "source-links"

    def __init__(
        self,
        repo_url: str = "https://github.com/michaelaye/planetarypy",
        branch: str = "main",
        source_dir: str = "src",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.repo_url = repo_url.rstrip("/")
        self.branch = branch
        self.source_dir = source_dir

    def _get_source_link(self, obj) -> str:
        """Generate a GitHub source link for a griffe object.

        Parameters
        ----------
        obj
            A griffe object (Function, Class, Module, etc.)

        Returns
        -------
        str
            Markdown link to the source on GitHub, or empty string if
            line number info is unavailable.
        """
        # Handle Alias objects by getting the target
        if hasattr(obj, "target") and obj.target is not None:
            obj = obj.target

        # Get line number - required for source link
        lineno = getattr(obj, "lineno", None)
        if lineno is None:
            return ""

        # Get the file path relative to the package
        # griffe provides relative_package_filepath which is like "planetarypy/config.py"
        rel_path = getattr(obj, "relative_package_filepath", None)
        if rel_path is None:
            # Fallback: try to get filepath and extract relative part
            filepath = getattr(obj, "filepath", None)
            if filepath is None:
                return ""
            # Try to find the package in the path
            filepath_str = str(filepath)
            if "planetarypy" in filepath_str:
                idx = filepath_str.find("planetarypy")
                rel_path = filepath_str[idx:]
            else:
                return ""

        # Build the full path: src/planetarypy/module.py
        full_path = f"{self.source_dir}/{rel_path}"

        # Build the URL with line numbers
        url = f"{self.repo_url}/blob/{self.branch}/{full_path}#L{lineno}"

        # Add end line if available for multi-line definitions
        endlineno = getattr(obj, "endlineno", None)
        if endlineno is not None and endlineno != lineno:
            url += f"-L{endlineno}"

        return f'\n\n[[source]]({url}){{.source-link target="_blank"}}'

    @dispatch
    def render(self, el: Union[layout.DocFunction, layout.DocAttribute]):
        """Render function/attribute documentation with source link."""
        # Get the base rendering from parent class
        base_render = super().render(el)

        # Add source link
        source_link = self._get_source_link(el.obj)

        return base_render + source_link

    @dispatch
    def render(self, el: Union[layout.DocClass, layout.DocModule]):
        """Render class/module documentation with source link."""
        # Get the base rendering from parent class
        base_render = super().render(el)

        # Add source link at the end of the class/module header section
        source_link = self._get_source_link(el.obj)

        if source_link:
            # Insert source link after the signature but before members
            # Find the first double newline after the signature
            parts = base_render.split("\n\n", 3)
            if len(parts) >= 3:
                # Insert after title and signature
                return parts[0] + "\n\n" + parts[1] + source_link + "\n\n" + "\n\n".join(parts[2:])

        return base_render + source_link


# Export as Renderer for quartodoc to discover
Renderer = SourceLinkMdRenderer
