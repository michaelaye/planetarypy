"""AST-based parser for pdr-tests selection_rules.py and CSV files.

Safely extracts the file_information dictionary from selection_rules.py
without executing any code, using Python's ast module.
"""

import ast
import csv
from pathlib import Path

from loguru import logger


def _eval_node(node: ast.expr, variables: dict[str, str]) -> object:
    """Safely evaluate an AST node to a Python value.

    Handles: Constant, Name (variable lookup), List, Dict, and UnaryOp (negation).
    """
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        if node.id in variables:
            return variables[node.id]
        if node.id == "True":
            return True
        if node.id == "False":
            return False
        if node.id == "None":
            return None
        logger.warning(f"Unknown variable reference: {node.id}")
        return node.id
    if isinstance(node, ast.List):
        return [_eval_node(el, variables) for el in node.elts]
    if isinstance(node, ast.Dict):
        return {
            _eval_node(k, variables): _eval_node(v, variables)
            for k, v in zip(node.keys, node.values)
        }
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        return -_eval_node(node.operand, variables)
    if isinstance(node, ast.Tuple):
        return tuple(_eval_node(el, variables) for el in node.elts)
    if isinstance(node, ast.Set):
        return {_eval_node(el, variables) for el in node.elts}
    if isinstance(node, ast.JoinedStr):
        # f-string — can't safely evaluate, return placeholder
        logger.warning("f-string encountered in AST, returning placeholder")
        return "<f-string>"
    logger.warning(f"Unhandled AST node type: {type(node).__name__}")
    return None


def parse_selection_rules(filepath: Path) -> dict[str, dict]:
    """Parse a selection_rules.py file and return the file_information dict.

    Uses ast.parse() to safely extract the dictionary without executing code.

    Parameters
    ----------
    filepath : Path
        Path to the selection_rules.py file

    Returns
    -------
    dict[str, dict]
        The file_information dictionary mapping product keys to their metadata
    """
    source = filepath.read_text(encoding="utf-8")
    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        logger.error(f"SyntaxError parsing {filepath}: {e}")
        return {}

    # Phase 1: collect module-level variable assignments (manifest aliases)
    variables: dict[str, str] = {}
    file_info = None

    for node in ast.iter_child_nodes(tree):
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if not isinstance(target, ast.Name):
                continue
            if target.id == "file_information":
                file_info = _eval_node(node.value, variables)
            elif isinstance(node.value, ast.Constant):
                variables[target.id] = node.value.value

    return file_info or {}


def parse_test_csv(filepath: Path) -> list[dict]:
    """Parse a pdr-tests CSV test file.

    Expected columns: label_file, files, product_id, url_stem, hash

    Parameters
    ----------
    filepath : Path
        Path to the CSV test file

    Returns
    -------
    list[dict]
        List of product entries as dictionaries
    """
    rows = []
    try:
        with filepath.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(dict(row))
    except Exception as e:
        logger.error(f"Error parsing CSV {filepath}: {e}")
    return rows


def list_definition_dirs(repo_path: Path) -> list[Path]:
    """List all instrument definition directories in the pdr-tests repo.

    Parameters
    ----------
    repo_path : Path
        Path to the cloned pdr-tests repository root

    Returns
    -------
    list[Path]
        Sorted list of definition directory paths that contain selection_rules.py
    """
    definitions_dir = repo_path / "pdr_tests" / "definitions"
    if not definitions_dir.exists():
        logger.error(f"Definitions directory not found: {definitions_dir}")
        return []

    dirs = sorted(
        d for d in definitions_dir.iterdir()
        if d.is_dir() and (d / "selection_rules.py").exists()
    )
    return dirs


def find_test_csvs(definition_dir: Path, product_keys: list[str]) -> dict[str, Path]:
    """Match CSV test files to product keys in a definition directory.

    Tries exact match first ({product_key}_test.csv), then case-insensitive.

    Parameters
    ----------
    definition_dir : Path
        Path to the instrument definition directory
    product_keys : list[str]
        Product keys from file_information dict

    Returns
    -------
    dict[str, Path]
        Mapping of product_key -> CSV file path for found matches
    """
    csv_files = {f.stem.lower(): f for f in definition_dir.glob("*_test.csv")}
    matches = {}

    for key in product_keys:
        expected = f"{key}_test".lower()
        if expected in csv_files:
            matches[key] = csv_files[expected]

    return matches
