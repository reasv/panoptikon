# Next.js standalone UI (monorepo ui/ or fetchFromGitHub via ui-pin.json).
# npm: importNpmLock reads package-lock.json (no separate npmDepsHash).
{
  lib,
  buildNpmPackage,
  importNpmLock,
  inter,
  version,
  uiSrc,
}:
assert lib.assertMsg (builtins.pathExists (uiSrc + "/package.json")) ''
  panoptikon: UI source has no package.json (${toString uiSrc}).
'';
assert lib.assertMsg (builtins.pathExists (uiSrc + "/package-lock.json")) ''
  panoptikon: UI source has no package-lock.json (${toString uiSrc}).
'';
  buildNpmPackage {
    pname = "panoptikon-ui";
    inherit version;
    src = uiSrc;

    npmDeps = importNpmLock {
      npmRoot = uiSrc;
    };
    npmConfigHook = importNpmLock.npmConfigHook;

    env = {
      BUILD_STANDALONE = "true";
      # Keep under typical GHA ubuntu-latest RAM (~7 GiB) for cold matrix builds.
      NODE_OPTIONS = "--max-old-space-size=4096";
      NEXT_TELEMETRY_DISABLED = "1";
    };

    makeCacheWritable = true;
    npmFlags = ["--include=dev"];

    # Fonts after npmConfigHook so node is on PATH for the layout patch.
    preBuild = ''
      mkdir -p app/fonts
      cp ${inter}/share/fonts/truetype/InterVariable.ttf app/fonts/InterVariable.ttf
      node ${./patch-ui-offline-font.mjs} app/layout.tsx
    '';

    buildPhase = ''
      runHook preBuild
      npm run build
      runHook postBuild
    '';

    installPhase = ''
      runHook preInstall
      test -f .next/standalone/server.js
      mkdir -p $out
      cp -a .next/standalone/. $out/
      cp -a .next/static $out/.next/static
      if [ -d public ]; then
        cp -a public $out/public
      fi
      runHook postInstall
    '';

    meta = {
      description = "Panoptikon web UI (Next.js standalone bundle)";
      license = lib.licenses.agpl3Plus;
    };
  }
