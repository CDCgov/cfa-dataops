"""Command line interface for creating a dataops catalog."""

import argparse
import os
import shutil
import sys

from mako.lookup import TemplateLookup

from .. import __version__
from ..catalog import cns as catalog_namespace

_here_dir = os.path.split(os.path.abspath(__file__))[0]
template_dirs = [os.path.join(_here_dir, "repo_templates")]
repo_files_dirs = os.path.join(_here_dir, "repo_files")
repo_template_lookup = TemplateLookup(
    directories=template_dirs, cache_enabled=False
)


def main():
    """Main entry point for the command line interface."""
    parser = argparse.ArgumentParser(description="Create a dataops catalog.")
    parser.add_argument(
        "unique_name",
        type=str,
        help=f"Unique module name to append to the catalog namespace ({catalog_namespace}.<unique_name>).",
    )
    parser.add_argument(
        "location",
        type=str,
        help="Where do you want to create this library locally?",
    )

    parser.add_argument(
        "--expanded_repo_for_cfa",
        "-x",
        action="store_true",
        help="Create an expanded repository structure for CFA.",
        default=False,
    )

    args = parser.parse_args()

    unique_name = args.unique_name.lower().replace("-", "_").replace(" ", "_")
    unique_name = "".join(c for c in unique_name if c.isalnum() or c == "_")

    namespace_ok = ""

    while namespace_ok.lower() not in ["y", "n"]:
        namespace_ok = input(
            f"This will create a new dataops catalog module named {catalog_namespace}.{unique_name}. Continue? (y/n) "
        )

    if namespace_ok.lower() != "y":
        print("Aborting.")
        sys.exit(0)

    location = os.path.abspath(args.location)

    if not os.path.exists(location):
        os.makedirs(location)
        print(f"Creating directory {location}")
        library_modules_root = os.path.join(
            location, *catalog_namespace.split("."), unique_name
        )
        os.makedirs(library_modules_root)

        with open(os.path.join(library_modules_root, "__init__.py"), "w") as f:
            template = repo_template_lookup.get_template("__init__.py.mako")
            f.write(
                template.render(
                    dataops_creation_version=__version__,
                    unique_name=unique_name,
                )
            )

        with open(
            os.path.join(library_modules_root, "catalog_defaults.toml"), "w"
        ) as f:
            template = repo_template_lookup.get_template(
                "catalog_defaults.toml.mako"
            )
            f.write(
                template.render(
                    unique_name=unique_name,
                )
            )

        with open(os.path.join(location, "MANIFEST.in"), "w") as f:
            template = repo_template_lookup.get_template("MANIFEST.in.mako")
            f.write(
                template.render(
                    lib_module_dir=os.path.join(
                        *catalog_namespace.split("."), unique_name
                    ),
                )
            )

        with open(os.path.join(location, "pyproject.toml"), "w") as f:
            template = repo_template_lookup.get_template("pyproject.toml.mako")
            f.write(
                template.render(
                    unique_name=unique_name,
                    catalog_namespace=catalog_namespace,
                )
            )

        for mod_i in ["datasets", "reports", "workflows"]:
            shutil.copytree(
                os.path.join(repo_files_dirs, mod_i),
                os.path.join(library_modules_root, mod_i),
            )

        for copy_i in [".gitignore"]:
            shutil.copy(
                os.path.join(repo_files_dirs, copy_i),
                os.path.join(location, copy_i),
            )

    else:
        print(f"Directory {location} already exists.")
        sys.exit(0)

    if args.expanded_repo_for_cfa:
        print(
            "Including CFA expanded repository pre-commit and workflow files."
        )
        for copy_i in [
            ".pre-commit-config.yaml",
            ".secrets.baseline",
            "DISCLAIMER.md",
            "LICENSE",
            "ruff.toml",
            ".gitattributes",
            ".github/",
        ]:
            shutil.copy(
                os.path.join(repo_files_dirs, "_cfa_only", copy_i),
                os.path.join(location, copy_i),
            )

    print(
        f"Created dataops catalog module {catalog_namespace}.{unique_name} at {location}"
        ". You can now 'cd' to this directory and run 'pip install -e .[dev]' to install the library in editable mode."
    )


if __name__ == "__main__":
    main()
