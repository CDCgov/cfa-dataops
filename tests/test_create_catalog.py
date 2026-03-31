"""Tests for the create_catalog submodule functionality."""

import os
import shutil

from cfa.dataops import __version__
from cfa.dataops.create_catalog.command import main

# Define catalog namespace directly from config
catalog_namespace = "cfa.catalog"


def create_catalog_programmatically(
    unique_name: str, location: str, expanded_repo_for_cfa: bool = False
):
    """
    Programmatic version of catalog creation for testing purposes.
    This bypasses the interactive input and command line parsing.
    """
    from mako.lookup import TemplateLookup

    _here_dir = os.path.split(os.path.abspath(__file__))[0]
    # Adjust path to point to the actual create_catalog directory
    create_catalog_dir = os.path.join(
        os.path.dirname(_here_dir), "cfa", "dataops", "create_catalog"
    )
    template_dirs = [os.path.join(create_catalog_dir, "repo_templates")]
    repo_files_dirs = os.path.join(create_catalog_dir, "repo_files")
    repo_template_lookup = TemplateLookup(
        directories=template_dirs, cache_enabled=False
    )

    # Clean and validate unique_name
    unique_name = unique_name.lower().replace("-", "_").replace(" ", "_")
    unique_name = "".join(c for c in unique_name if c.isalnum() or c == "_")

    location = os.path.abspath(location)

    if not os.path.exists(location):
        os.makedirs(location)
        library_modules_root = os.path.join(
            location, *catalog_namespace.split("."), unique_name
        )
        os.makedirs(library_modules_root)

        # Create __init__.py
        with open(os.path.join(library_modules_root, "__init__.py"), "w") as f:
            template = repo_template_lookup.get_template("__init__.py.mako")
            f.write(
                template.render(
                    dataops_creation_version=__version__,
                    unique_name=unique_name,
                )
            )

        # Create catalog_defaults.toml
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

        # Create MANIFEST.in
        with open(os.path.join(location, "MANIFEST.in"), "w") as f:
            template = repo_template_lookup.get_template("MANIFEST.in.mako")
            f.write(
                template.render(
                    lib_module_dir=os.path.join(
                        *catalog_namespace.split("."), unique_name
                    ),
                )
            )

        # Create pyproject.toml
        with open(os.path.join(location, "pyproject.toml"), "w") as f:
            template = repo_template_lookup.get_template("pyproject.toml.mako")
            f.write(
                template.render(
                    unique_name=unique_name,
                    catalog_namespace=catalog_namespace,
                )
            )

        # Copy module directories
        for mod_i in ["datasets", "reports", "workflows"]:
            shutil.copytree(
                os.path.join(repo_files_dirs, mod_i),
                os.path.join(library_modules_root, mod_i),
            )

        # Copy .gitignore
        for copy_i in [".gitignore"]:
            shutil.copy(
                os.path.join(repo_files_dirs, copy_i),
                os.path.join(location, copy_i),
            )

        # Handle expanded repo for CFA
        if expanded_repo_for_cfa:
            for copy_i in [
                ".pre-commit-config.yaml",
                ".secrets.baseline",
                "DISCLAIMER.md",
                "LICENSE",
                "ruff.toml",
                ".gitattributes",
            ]:
                shutil.copy(
                    os.path.join(repo_files_dirs, "_cfa_only", copy_i),
                    os.path.join(location, copy_i),
                )
            # Note: .github directory copy would need to exist in repo_files/_cfa_only
            github_src = os.path.join(repo_files_dirs, "_cfa_only", ".github")
            if os.path.exists(github_src):
                shutil.copytree(
                    github_src,
                    os.path.join(location, ".github"),
                )

        return True
    else:
        return False


def test_create_basic_catalog(catalog_parent):
    """Test creating a basic catalog with minimal configuration."""
    unique_name = "test_catalog"
    catalog_location = catalog_parent.join("basic_test")

    # Create the catalog
    success = create_catalog_programmatically(
        unique_name=unique_name,
        location=str(catalog_location),
        expanded_repo_for_cfa=False,
    )

    assert success is True

    # Verify directory structure
    assert catalog_location.check(dir=True)

    # Check main catalog module directory
    catalog_module_path = catalog_location.join("cfa", "catalog", unique_name)
    assert catalog_module_path.check(dir=True)

    # Check required files exist
    assert catalog_module_path.join("__init__.py").check(file=True)
    assert catalog_module_path.join("catalog_defaults.toml").check(file=True)
    assert catalog_location.join("pyproject.toml").check(file=True)
    assert catalog_location.join("MANIFEST.in").check(file=True)
    assert catalog_location.join(".gitignore").check(file=True)

    # Check module directories
    assert catalog_module_path.join("datasets").check(dir=True)
    assert catalog_module_path.join("reports").check(dir=True)
    assert catalog_module_path.join("workflows").check(dir=True)


def test_create_expanded_catalog(catalog_parent):
    """Test creating an expanded catalog with CFA-specific files."""
    unique_name = "expanded_test"
    catalog_location = catalog_parent.join("expanded_test")

    # Create the catalog with expanded repo
    success = create_catalog_programmatically(
        unique_name=unique_name,
        location=str(catalog_location),
        expanded_repo_for_cfa=True,
    )

    assert success is True

    # Verify basic structure
    assert catalog_location.check(dir=True)
    catalog_module_path = catalog_location.join("cfa", "catalog", unique_name)
    assert catalog_module_path.check(dir=True)

    # Check CFA-specific files
    cfa_files = [
        ".pre-commit-config.yaml",
        ".secrets.baseline",
        "DISCLAIMER.md",
        "LICENSE",
        "ruff.toml",
        ".gitattributes",
    ]

    for file_name in cfa_files:
        assert catalog_location.join(file_name).check(
            file=True
        ), f"Missing CFA file: {file_name}"


def test_unique_name_sanitization(catalog_parent):
    """Test that unique names are properly sanitized."""
    test_cases = [
        ("Test-Catalog", "test_catalog"),
        ("test catalog", "test_catalog"),
        ("Test_Catalog123", "test_catalog123"),
        ("test@#$%catalog", "testcatalog"),
    ]

    for i, (input_name, expected_name) in enumerate(test_cases):
        # Use unique directory for each test case
        catalog_location = catalog_parent.join(
            f"sanitize_test_{i}_{expected_name}"
        )

        success = create_catalog_programmatically(
            unique_name=input_name,
            location=str(catalog_location),
            expanded_repo_for_cfa=False,
        )

        assert success is True

        # Check that the directory uses the sanitized name
        catalog_module_path = catalog_location.join(
            "cfa", "catalog", expected_name
        )
        assert catalog_module_path.check(dir=True)


def test_catalog_file_contents(catalog_parent):
    """Test that generated files contain expected content."""
    unique_name = "content_test"
    catalog_location = catalog_parent.join("content_test")

    success = create_catalog_programmatically(
        unique_name=unique_name,
        location=str(catalog_location),
        expanded_repo_for_cfa=False,
    )

    assert success is True

    catalog_module_path = catalog_location.join("cfa", "catalog", unique_name)

    # Check __init__.py content
    init_content = catalog_module_path.join("__init__.py").read()
    assert f"_dataops_creation_version = '{__version__}'" in init_content
    assert f"_catalog_ns = '{unique_name}'" in init_content

    # Check pyproject.toml content
    pyproject_content = catalog_location.join("pyproject.toml").read()
    assert f'name = "{catalog_namespace}.{unique_name}"' in pyproject_content
    assert "cfa-dataops" in pyproject_content

    # Check MANIFEST.in content
    manifest_content = catalog_location.join("MANIFEST.in").read()
    assert f"cfa/catalog/{unique_name}" in manifest_content


def test_datasets_directory_structure(catalog_parent):
    """Test that the datasets directory is properly created with example files."""
    unique_name = "datasets_test"
    catalog_location = catalog_parent.join("datasets_test")

    success = create_catalog_programmatically(
        unique_name=unique_name,
        location=str(catalog_location),
        expanded_repo_for_cfa=False,
    )

    assert success is True

    datasets_path = catalog_location.join(
        "cfa", "catalog", unique_name, "datasets"
    )
    assert datasets_path.check(dir=True)
    assert datasets_path.join("__init__.py").check(file=True)

    # Check for example dataset files
    example_files = [
        "etl_example.toml",
        "experiment_tracking_example.toml",
        "multistage_example.toml",
        "reference_data_example.toml",
    ]

    for example_file in example_files:
        assert datasets_path.join(example_file).check(
            file=True
        ), f"Missing example file: {example_file}"


def test_reports_directory_structure(catalog_parent):
    """Test that the reports directory is properly created."""
    unique_name = "reports_test"
    catalog_location = catalog_parent.join("reports_test")

    success = create_catalog_programmatically(
        unique_name=unique_name,
        location=str(catalog_location),
        expanded_repo_for_cfa=False,
    )

    assert success is True

    reports_path = catalog_location.join(
        "cfa", "catalog", unique_name, "reports"
    )
    assert reports_path.check(dir=True)
    assert reports_path.join("__init__.py").check(file=True)

    # Check for examples directory
    examples_path = reports_path.join("examples")
    assert examples_path.check(dir=True)
    assert examples_path.join("basics.ipynb").check(file=True)


def test_workflows_directory_structure(catalog_parent):
    """Test that the workflows directory is properly created."""
    unique_name = "workflows_test"
    catalog_location = catalog_parent.join("workflows_test")

    success = create_catalog_programmatically(
        unique_name=unique_name,
        location=str(catalog_location),
        expanded_repo_for_cfa=False,
    )

    assert success is True

    workflows_path = catalog_location.join(
        "cfa", "catalog", unique_name, "workflows"
    )
    assert workflows_path.check(dir=True)
    assert workflows_path.join("__init__.py").check(file=True)

    # Check for workflow subdirectories
    workflow_dirs = ["etl", "multistage", "reference_data"]
    for workflow_dir in workflow_dirs:
        workflow_path = workflows_path.join(workflow_dir)
        assert workflow_path.check(dir=True)
        assert workflow_path.join("__init__.py").check(file=True)


def test_existing_directory_handling(catalog_parent):
    """Test behavior when target directory already exists."""
    unique_name = "existing_test"
    catalog_location = catalog_parent.join("existing_test")

    # Create the directory first
    catalog_location.mkdir()

    # Try to create catalog in existing directory
    success = create_catalog_programmatically(
        unique_name=unique_name,
        location=str(catalog_location),
        expanded_repo_for_cfa=False,
    )

    # Should return False for existing directory
    assert success is False


def test_main_function_with_confirmation(mocker, catalog_parent):
    """Test the main CLI function with mocked user input."""
    unique_name = "cli_test"
    catalog_location = catalog_parent.join("cli_test")

    # Mock user input and sys.exit
    mocker.patch("builtins.input", return_value="y")
    mock_exit = mocker.patch("sys.exit")

    # Mock sys.argv to simulate command line arguments
    mocker.patch(
        "sys.argv", ["create-catalog", unique_name, str(catalog_location)]
    )

    # Call main function - it should create the catalog successfully
    main()

    # Verify sys.exit was not called (success case)
    mock_exit.assert_not_called()

    # Verify the catalog was actually created
    assert catalog_location.check(dir=True)
    catalog_module_path = catalog_location.join("cfa", "catalog", unique_name)
    assert catalog_module_path.check(dir=True)
    assert catalog_module_path.join("__init__.py").check(file=True)


def test_main_function_with_abort(mocker, catalog_parent):
    """Test the main CLI function when user aborts."""
    unique_name = "abort_test"
    catalog_location = catalog_parent.join("abort_test")

    # Mock user input and sys.exit
    mocker.patch("builtins.input", return_value="n")
    mock_exit = mocker.patch("sys.exit")

    # Mock sys.argv to simulate command line arguments
    mocker.patch(
        "sys.argv", ["create-catalog", unique_name, str(catalog_location)]
    )

    # Call main function
    main()

    # Verify sys.exit was called with 0 (abort case)
    mock_exit.assert_called_once_with(0)


def test_catalog_namespace_constant():
    """Test that the catalog namespace constant is correctly imported."""
    assert catalog_namespace == "cfa.catalog"


def test_version_import():
    """Test that version is properly imported and available."""
    assert __version__ is not None
    assert isinstance(__version__, str)


def test_template_rendering(catalog_parent):
    """Test that Mako templates are properly rendered with correct variables."""
    unique_name = "template_test"
    catalog_location = catalog_parent.join("template_test")

    success = create_catalog_programmatically(
        unique_name=unique_name,
        location=str(catalog_location),
        expanded_repo_for_cfa=False,
    )

    assert success is True

    catalog_module_path = catalog_location.join("cfa", "catalog", unique_name)

    # Test __init__.py template rendering
    init_content = catalog_module_path.join("__init__.py").read()
    assert "_dataops_creation_version" in init_content
    assert "_catalog_ns" in init_content

    # Test catalog_defaults.toml template rendering
    defaults_content = catalog_module_path.join("catalog_defaults.toml").read()
    assert (
        unique_name in defaults_content or "[" in defaults_content
    )  # Should contain TOML structure

    # Test pyproject.toml template rendering
    pyproject_content = catalog_location.join("pyproject.toml").read()
    assert "[project]" in pyproject_content
    assert unique_name in pyproject_content
    assert catalog_namespace in pyproject_content


def test_file_copying_operations(catalog_parent):
    """Test that files are properly copied from repo_files directory."""
    unique_name = "copy_test"
    catalog_location = catalog_parent.join("copy_test")

    success = create_catalog_programmatically(
        unique_name=unique_name,
        location=str(catalog_location),
        expanded_repo_for_cfa=False,
    )

    assert success is True

    catalog_module_path = catalog_location.join("cfa", "catalog", unique_name)

    # Verify .gitignore was copied
    gitignore_path = catalog_location.join(".gitignore")
    assert gitignore_path.check(file=True)

    # Verify module directories were copied with their contents
    for module_dir in ["datasets", "reports", "workflows"]:
        module_path = catalog_module_path.join(module_dir)
        assert module_path.check(dir=True)
        assert module_path.join("__init__.py").check(file=True)
