{
  description = "a boring-semantic-layer";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

    pyproject-nix = {
      url = "github:pyproject-nix/pyproject.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    uv2nix = {
      url = "github:pyproject-nix/uv2nix";
      inputs.pyproject-nix.follows = "pyproject-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    pyproject-build-systems = {
      url = "github:pyproject-nix/build-system-pkgs";
      inputs.pyproject-nix.follows = "pyproject-nix";
      inputs.uv2nix.follows = "uv2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = {
    self,
    nixpkgs,
    uv2nix,
    pyproject-nix,
    pyproject-build-systems,
    ...
  }: let
    inherit (nixpkgs) lib;

    # Support both x86_64-linux and aarch64-darwin
    supportedSystems = ["x86_64-linux" "aarch64-darwin"];
    forAllSystems = lib.genAttrs supportedSystems;
    pkgsFor = system: nixpkgs.legacyPackages.${system};

    # Load a uv workspace from a workspace root.
    # Uv2nix treats all uv projects as workspace projects.
    workspace = uv2nix.lib.workspace.loadWorkspace {workspaceRoot = ./.;};

    # Create package overlay from workspace.
    overlay = workspace.mkPyprojectOverlay {
      # Always prefer prebuilt binary wheels as a package source
      sourcePreference = "wheel"; # or sourcePreference = "sdist";
    };

    # Helper function to create Python set for a specific system
    mkPythonSetForSystem = system: let
      pkgs = pkgsFor system;
      python = pkgs.python312;

      # Package-specific overrides
      pyprojectOverrides = final: prev: {
        # Build system dependencies for cityhash
        cityhash = prev.cityhash.overrideAttrs (old: {
          nativeBuildInputs =
            (old.nativeBuildInputs or [])
            ++ final.resolveBuildSystem {
              setuptools = [];
              wheel = [];
            };
        });

        # Scientific computing packages
        scikit-learn = prev.scikit-learn.overrideAttrs (old: {
          nativeBuildInputs =
            (old.nativeBuildInputs or [])
            ++ [
              final.meson-python
              final.ninja
              final.cython
              final.numpy
              final.scipy
              final.packaging
              final.pyproject-metadata
              final.gast
              pkgs.pkg-config
              pkgs.openblas
              pkgs.meson
            ];
        });

        scipy = prev.scipy.overrideAttrs (old: {
          nativeBuildInputs =
            (old.nativeBuildInputs or [])
            ++ [
              final.meson-python
              final.ninja
              final.cython
              final.numpy
              final.pybind11
              final.pythran
              final.packaging
              final.pyproject-metadata
              final.gast
              final.beniget
              final.ply
            ];

          buildInputs =
            (old.buildInputs or [])
            ++ [
              pkgs.gfortran
              pkgs.cmake
              pkgs.xsimd
              pkgs.pkg-config
              pkgs.openblas
              pkgs.meson
              pkgs.lapack
            ];
        });

        # Google packages
        google-crc32c = prev.google-crc32c.overrideAttrs (old: {
          nativeBuildInputs =
            (old.nativeBuildInputs or [])
            ++ final.resolveBuildSystem {
              setuptools = [];
              wheel = [];
            };
        });

        # Data processing packages
        pyarrow = prev.pyarrow.overrideAttrs (old: {
          nativeBuildInputs =
            (old.nativeBuildInputs or [])
            ++ [
              final.setuptools
              final.cython
              final.numpy
              pkgs.cmake
              pkgs.pkg-config
            ];
          buildInputs =
            (old.buildInputs or [])
            ++ [
              pkgs.arrow-cpp
              pkgs.lz4
            ];
        });

        # Machine learning packages
        xgboost = prev.xgboost.overrideAttrs (old: {
          nativeBuildInputs =
            (old.nativeBuildInputs or [])
            ++ [pkgs.cmake]
            ++ final.resolveBuildSystem (
              pkgs.lib.listToAttrs (map (name: pkgs.lib.nameValuePair name []) ["hatchling"])
            );
        });

        # Custom package override for xorq
        xorq = prev.xorq.overrideAttrs (old: {
          nativeBuildInputs =
            (old.nativeBuildInputs or [])
            ++ final.resolveBuildSystem {
              setuptools = [];
              wheel = [];
            };
          buildInputs = (old.buildInputs or []) ++ [pkgs.openssl];
        });

        # Network/RPC packages
        grpcio = prev.grpcio.overrideAttrs (old: {
          NIX_CFLAGS_COMPILE =
            (old.NIX_CFLAGS_COMPILE or "")
            + " -DTARGET_OS_OSX=1 -D_DARWIN_C_SOURCE"
            + " -I${pkgs.zlib.dev}/include"
            + " -I${pkgs.openssl.dev}/include"
            + " -I${pkgs.c-ares.dev}/include";

          NIX_LDFLAGS =
            (old.NIX_LDFLAGS or "")
            + " -L${pkgs.zlib.out}/lib -lz"
            + " -L${pkgs.openssl.out}/lib -lssl -lcrypto"
            + " -L${pkgs.c-ares.out}/lib -lcares";

          buildInputs =
            (old.buildInputs or [])
            ++ [
              pkgs.zlib
              pkgs.openssl
              pkgs.c-ares
            ];

          nativeBuildInputs =
            (old.nativeBuildInputs or [])
            ++ [
              pkgs.pkg-config
              pkgs.cmake
            ];

          # Environment variables for grpcio build
          GRPC_PYTHON_BUILD_SYSTEM_OPENSSL = "1";
          GRPC_PYTHON_BUILD_SYSTEM_ZLIB = "1";
          GRPC_PYTHON_BUILD_SYSTEM_CARES = "1";

          preBuild = ''
            export PYTHONPATH=${final.setuptools}/${python.sitePackages}:$PYTHONPATH
          '';
        });

        # Database packages
        duckdb = prev.duckdb.overrideAttrs (old: {
          nativeBuildInputs =
            (old.nativeBuildInputs or [])
            ++ [
              final.setuptools
              final.pybind11
              final.wheel
              pkgs.cmake
            ];
          buildInputs = (old.buildInputs or []) ++ [pkgs.openssl];
        });

        psycopg2-binary = prev.psycopg2-binary.overrideAttrs (old: {
          nativeBuildInputs =
            (old.nativeBuildInputs or [])
            ++ [
              final.setuptools
              final.wheel
              pkgs.postgresql.pg_config
              pkgs.postgresql
            ];
          buildInputs = (old.buildInputs or []) ++ [pkgs.openssl];
        });
      };
    in
      # Use base package set from pyproject.nix builders
      (pkgs.callPackage pyproject-nix.build.packages {
        inherit python;
      })
      .overrideScope (
        lib.composeManyExtensions [
          pyproject-build-systems.overlays.default
          overlay
          pyprojectOverrides
        ]
      );
  in {
    # Package outputs
    packages = forAllSystems (system: let
      pkgs = pkgsFor system;
    in {
      default = (mkPythonSetForSystem system).mkVirtualEnv "" workspace.deps.default;
    });

    # Application outputs
    apps = forAllSystems (system: {
      default = {
        type = "app";
        program = "${self.packages.${system}.default}/bin/hello";
      };
    });

    # Development shell outputs
    devShells = forAllSystems (system: let
      pkgs = pkgsFor system;
      python = pkgs.python312;
      pythonSet = mkPythonSetForSystem system;
    in {
      # Impure development shell using system uv
      impure = pkgs.mkShell {
        packages = [
          python
          pkgs.uv
        ];

        env =
          {
            # Prevent uv from managing Python downloads
            UV_PYTHON_DOWNLOADS = "never";
            # Force uv to use nixpkgs Python interpreter
            UV_PYTHON = python.interpreter;
          }
          // lib.optionalAttrs pkgs.stdenv.isLinux {
            # Python libraries often load native shared objects using dlopen(3).
            # Setting LD_LIBRARY_PATH makes the dynamic library loader aware of libraries without using RPATH for lookup.
            LD_LIBRARY_PATH = lib.makeLibraryPath pkgs.pythonManylinuxPackages.manylinux1;
          };

        shellHook = ''
          unset PYTHONPATH
        '';
      };

      # Pure development shell using uv2nix with editable installs
      editable = let
        # Create an overlay enabling editable mode for all local dependencies.
        editableOverlay = workspace.mkEditablePyprojectOverlay {
          # Use environment variable
          root = "$REPO_ROOT";
        };

        # Override previous set with our editable overlay.
        editablePythonSet = pythonSet.overrideScope (
          lib.composeManyExtensions [
            editableOverlay

            # Apply fixups for building an editable package of your workspace packages
            (final: prev: {
              boring-semantic-layer = prev.boring-semantic-layer.overrideAttrs (old: {
                # Filter the sources going into an editable build
                # so the editable package doesn't have to be rebuilt on every change.
                src = lib.fileset.toSource {
                  root = old.src;
                  fileset = lib.fileset.unions [
                    (old.src + "/pyproject.toml")
                    (old.src + "/README.md")
                    (old.src + "/src/boring_semantic_layer/__init__.py")
                  ];
                };

                # Hatchling (our build system) has a dependency on the `editables` package when building editables.
                nativeBuildInputs =
                  old.nativeBuildInputs
                  ++ final.resolveBuildSystem {
                    editables = [];
                  };
              });
            })
          ]
        );

        # Build virtual environment, with local packages being editable.
        virtualenv = editablePythonSet.mkVirtualEnv "boring-semantic-layer-dev-env" workspace.deps.all;
      in
        pkgs.mkShell {
          packages = [
            virtualenv
            pkgs.uv
          ];

          env = {
            # Don't create venv using uv
            UV_NO_SYNC = "1";
            # Force uv to use Python interpreter from venv
            UV_PYTHON = "${virtualenv}/bin/python";
            # Prevent uv from downloading managed Python's
            UV_PYTHON_DOWNLOADS = "never";
          };

          shellHook = ''
            # Undo dependency propagation by nixpkgs.
            unset PYTHONPATH

            # Get repository root using git. This is expanded at runtime by the editable `.pth` machinery.
            export REPO_ROOT=$(git rev-parse --show-toplevel)
          '';
        };
    });
  };
}
