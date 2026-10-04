{
  lib,
  stdenv,
  rustPlatform,
  pkg-config,
  openssl,
  makeWrapper,
  wrapGAppsHook4,
  copyDesktopItems,
  makeDesktopItem,
  gtk3,
  glib,
  webkitgtk_4_1,
  libsoup_3,
  librsvg,
  libayatana-appindicator,
  glib-networking,
  gst_all_1,
  runCommand,
  fetchurl,
  python3,
  # Server sidecar (overridable). Default desktop uses panoptikon (config-follow).
  panoptikon,
  src,
  # Track the server package (and thus panoptikon/Cargo.toml) by default.
  version ? panoptikon.version,
}: let
  pname = "panoptikon-desktop";
  rustTarget = stdenv.hostPlatform.rust.rustcTarget;
  sidecarName = "panoptikon-${rustTarget}";
  # Tauri Linux resource_dir is ../lib/<productName> from the executable.
  desktopProductName = (builtins.fromJSON (builtins.readFile ../../../../panoptikon-desktop/src-tauri/tauri.conf.json)).productName;
  pdfiumLock = builtins.fromJSON (builtins.readFile ../../../pdfium/pdfium-lock.json);
  pdfiumSpec = pdfiumLock.targets.${rustTarget} or null;
  pdfiumWheel =
    if pdfiumSpec == null
    then null
    else
      fetchurl {
        url = pdfiumSpec.url;
        hash = "sha256:${pdfiumSpec.sha256}";
      };
  pdfiumLibraryName =
    if pdfiumSpec == null
    then "libpdfium.so"
    else pdfiumSpec.library;

  # Must match contrib/package/common/share/icons/hicolor (generate-hicolor-icons.sh).
  iconSizes = [
    16
    22
    24
    32
    48
    64
    96
    128
    256
    512
  ];

  trayLibPath = lib.makeLibraryPath [
    libayatana-appindicator
    gtk3
    glib
  ];
in
  rustPlatform.buildRustPackage (finalAttrs: {
    inherit pname version src;

    cargoLock.lockFile = src + "/Cargo.lock";

    cargoBuildFlags = [
      "-p"
      "panoptikon-desktop"
    ];
    doCheck = false;

    # Nix delivers updates: compile out the in-app updater.
    env.PANOPTIKON_PACKAGE_MANAGED_UPDATES = "1";

    nativeBuildInputs = [
      pkg-config
      makeWrapper
      wrapGAppsHook4
      copyDesktopItems
      python3
    ];

    buildInputs =
      [
        openssl
        gtk3
        webkitgtk_4_1
        libsoup_3
        librsvg
        libayatana-appindicator
        glib-networking
      ]
      ++ (with gst_all_1; [
        gstreamer
        gst-plugins-base
        gst-plugins-good
      ]);

    desktopItems = [
      (makeDesktopItem {
        name = "panoptikon-desktop";
        desktopName = "Panoptikon Desktop";
        genericName = "Media Search";
        comment = "Local multimodal media search";
        exec = "panoptikon-desktop";
        icon = "panoptikon-desktop";
        terminal = false;
        startupNotify = false;
        # One main category (Graphics); Viewer is additional. Multiple mains
        # make desktop-file-validate warn about duplicate menu entries.
        categories = [
          "Graphics"
          "Viewer"
        ];
        keywords = [
          "media"
          "search"
          "photos"
          "images"
          "video"
        ];
      })
    ];

    preConfigure =
      ''
        mkdir -p panoptikon-desktop/src-tauri/binaries
        cp -f ${panoptikon}/bin/panoptikon \
          panoptikon-desktop/src-tauri/binaries/${sidecarName}
        chmod +x panoptikon-desktop/src-tauri/binaries/${sidecarName}
      ''
      + (
        if pdfiumWheel == null
        then ''
          echo "contrib/pdfium/pdfium-lock.json has no PDFium wheel for ${rustTarget}" >&2
          exit 1
        ''
        else ''
          # postInstall bundles the staged PDFium.
          # Same pinned wheel as scripts/stage-pdfium.py; the fetch is fixed-output.
          mkdir -p "$NIX_BUILD_TOP/pdfium-cache"
          cp ${pdfiumWheel} "$NIX_BUILD_TOP/pdfium-cache/${baseNameOf pdfiumSpec.url}"
          python3 scripts/stage-pdfium.py \
            --target ${rustTarget} \
            --cache "$NIX_BUILD_TOP/pdfium-cache" \
            --output "$PWD/panoptikon-desktop/src-tauri/resources/pdfium"
        ''
      );

    postPatch = ''
      substituteInPlace panoptikon-desktop/src-tauri/tauri.conf.json \
        --replace-fail '"createUpdaterArtifacts": true' \
                       '"createUpdaterArtifacts": false'
    '';

    dontWrapGApps = true;

    postInstall = ''
      install -Dm755 ${panoptikon}/bin/panoptikon $out/bin/panoptikon
      install -Dm755 ${panoptikon}/bin/panoptikon \
        $out/libexec/panoptikon-desktop/${sidecarName}

      # Server config seeds (same as server package) for `panoptikon` from this
      # closure; tray app may still manage its own state under the user profile.
      mkdir -p $out/share/panoptikon
      cp -a ${panoptikon}/share/panoptikon/. $out/share/panoptikon/

      # Common packaging assets (contrib/package/common); not re-rendered.
      for size in ${lib.concatMapStringsSep " " toString iconSizes}; do
        install -Dm644 \
          contrib/package/common/share/icons/hicolor/''${size}x''${size}/apps/panoptikon-desktop.png \
          $out/share/icons/hicolor/''${size}x''${size}/apps/panoptikon-desktop.png
      done
      install -Dm644 \
        contrib/package/common/share/icons/hicolor/scalable/apps/panoptikon-desktop.svg \
        $out/share/icons/hicolor/scalable/apps/panoptikon-desktop.svg

      # Unpackaged Linux builds resolve resources at $out/lib/<productName>
      # (exe is $out/bin/.panoptikon-desktop-wrapped after wrapProgram).
      mkdir -p "$out/lib/${desktopProductName}"
      cp -a panoptikon-desktop/src-tauri/resources/pdfium \
        "$out/lib/${desktopProductName}/pdfium"
    '';

    postFixup = ''
      wrapProgram $out/bin/panoptikon-desktop \
        "''${gappsWrapperArgs[@]}" \
        --prefix PATH : $out/bin \
        --prefix LD_LIBRARY_PATH : ${trayLibPath} \
        --set-default WEBKIT_DISABLE_COMPOSITING_MODE 1
    '';

    passthru = {
      inherit panoptikon;
      tests = {
        install =
          runCommand "panoptikon-desktop-test-install"
          {
            meta.timeout = 60;
          }
          ''
            pkg=${finalAttrs.finalPackage}
            test -x "$pkg/bin/panoptikon-desktop"
            test -x "$pkg/bin/panoptikon"
            test -x "$pkg/libexec/panoptikon-desktop/${sidecarName}"
            grep -q UV_PYTHON "$pkg/bin/panoptikon"
            grep -q PATH "$pkg/bin/panoptikon-desktop"
            grep -q libayatana-appindicator "$pkg/bin/panoptikon-desktop"
            cmp -s "$pkg/bin/panoptikon" ${panoptikon}/bin/panoptikon

            desktop="$pkg/share/applications/panoptikon-desktop.desktop"
            test -f "$desktop"
            grep -q '^Exec=panoptikon-desktop' "$desktop"
            grep -q '^Icon=panoptikon-desktop' "$desktop"
            grep -q '^Name=Panoptikon Desktop' "$desktop"

            test -f "$pkg/share/icons/hicolor/scalable/apps/panoptikon-desktop.svg"
            test -s "$pkg/lib/${desktopProductName}/pdfium/${pdfiumLibraryName}"
            test -s "$pkg/lib/${desktopProductName}/pdfium/manifest.json"
            for size in ${lib.concatMapStringsSep " " toString iconSizes}; do
              icon="$pkg/share/icons/hicolor/''${size}x''${size}/apps/panoptikon-desktop.png"
              test -s "$icon"
            done
            # Sidecar GPU wrap must match the linked server package flags.
            ${
              if panoptikon.cudaSupport or false
              then ''
                grep -q cuda "$pkg/bin/panoptikon"
                grep -q PANOPTIKON_ACCELERATOR "$pkg/bin/panoptikon"
              ''
              else if panoptikon.rocmSupport or false
              then ''
                grep -q rocm "$pkg/bin/panoptikon"
                grep -q PANOPTIKON_ACCELERATOR "$pkg/bin/panoptikon"
              ''
              else ''
                ! grep -q PANOPTIKON_ACCELERATOR "$pkg/bin/panoptikon"
              ''
            }
            touch $out
          '';
      };
    };

    meta = {
      description = "Panoptikon Desktop tray app (Tauri) with bundled Server sidecar";
      longDescription = ''
        Tauri tray app; server sidecar is the overridable panoptikon argument
        (PATH + externalBin). Default desktop follows nixpkgs GPU config via
        panoptikon; use -cpu/-cuda/-rocm desktop attrs for forced backends.
        Ships a FreeDesktop .desktop entry and hicolor icons from
        contrib/package/common/share/icons (source mark: static/logo_simple.svg).
      '';
      homepage = "https://github.com/reasv/panoptikon";
      license = lib.licenses.agpl3Plus;
      mainProgram = "panoptikon-desktop";
      platforms = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      maintainers = [];
    };
  })
