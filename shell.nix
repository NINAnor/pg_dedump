with import <nixpkgs> {
};

let
  unstable = import (builtins.fetchTarball https://github.com/NixOS/nixpkgs/tarball/b69de56fac8c2b6f8fd27f2eca01dcda8e0a4221) {};
  pythonPackages = unstable.python312Packages;

in pkgs.mkShell rec {
  name = "impurePythonEnv";
  venvDir = "./.venv";
  buildInputs = [
    # A Python interpreter including the 'venv' module is required to bootstrap
    # the environment.
    pythonPackages.python

    # This executes some shell code to initialize a venv in $venvDir before
    # dropping into the shell
    pythonPackages.venvShellHook
    pythonPackages.django-environ
    pythonPackages.click

    python312Packages.duckdb
    unstable.gdal
    unstable.duckdb
  ];

  # Run this command, only after creating the virtual environment
  postVenvCreation = ''
    unset SOURCE_DATE_EPOCH
    pip install -e .
  '';

  # Now we can execute any commands within the virtual environment.
  # This is optional and can be left out to run pip manually.
  postShellHook = ''
    # allow pip to install wheels
    unset SOURCE_DATE_EPOCH
  '';

}
