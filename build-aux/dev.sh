#!/usr/bin/env bash
# Incremental Flatpak builds on the host, for this machine's architecture,
# the other one (x86_64 or aarch64), or both.
#
# Prompts for the architecture(s), what to produce and the build profile,
# then writes .flatpak bundles to dist/ and/or installs the build for the
# current user. Ripples has no bundled libraries, so the SDK is set up once
# per architecture (and again when the manifest changes) and Ripples itself
# is compiled with Cargo inside it; only changed code is rebuilt. Crates
# download into a cache shared by both architectures; compiled output is
# kept per architecture and per profile. A Flatpak bundle holds one
# architecture, so "Both" writes one file per architecture. The aarch64
# bundle is the one to install on the phone.
#
# Building for the other architecture runs its SDK under QEMU user
# emulation, so the first build there is slow.

set -euo pipefail

cd "$(dirname "$0")/.."

APP_ID=net.dunevent.Ripples
MANIFEST=build-aux/$APP_ID.json
MODULE=ripples
BINARY=ripples
RUNTIME_REPO=https://dl.flathub.org/repo/flathub.flatpakrepo
DIST=dist

host_arch=$(flatpak --default-arch)
other_arch=$([[ $host_arch == x86_64 ]] && echo aarch64 || echo x86_64)

choose() {
    local prompt=$1 default=$2
    shift 2
    local i=1
    for option in "$@"; do
        echo "  $i) $option" >&2
        i=$((i + 1))
    done
    local reply
    read -rp "$prompt [$default]: " reply
    reply=${reply:-$default}
    if [[ ! $reply =~ ^[0-9]+$ ]] || ((reply < 1 || reply > $#)); then
        echo "Unknown choice: $reply" >&2
        exit 2
    fi
    echo "${!reply}"
}

echo "Architecture:"
# Assigned before matching, because a failed choice inside `case $(...)`
# does not stop the script.
arch_choice=$(choose "Choose" 1 "$host_arch (this machine)" "$other_arch" "Both")
case $arch_choice in
    Both) arches=("$host_arch" "$other_arch") ;;
    "$other_arch") arches=("$other_arch") ;;
    *) arches=("$host_arch") ;;
esac

# A build for another architecture cannot be installed here, so it always
# produces a file, and "Install only" is offered only for this machine.
if [[ ${#arches[@]} -eq 1 && ${arches[0]} != "$host_arch" ]]; then
    target="Flatpak file"
elif [[ ${#arches[@]} -eq 2 ]]; then
    echo "Target:"
    target_choice=$(choose "Choose" 1 "Flatpak files" "Flatpak files and install")
    case $target_choice in
        "Flatpak files") target="Flatpak file" ;;
        *) target="Flatpak file and install" ;;
    esac
else
    echo "Target:"
    target=$(choose "Choose" 1 "Flatpak file" "Install only" "Flatpak file and install")
fi

# Ripples' Cargo.toml defines no profiles, so the tiers are the stock dev and
# release profiles with release settings overridden through the environment.
# Each tier gets its own target directory, since the overrides change what
# "release" means and would otherwise force a full rebuild on every switch.
echo "Build profile:"
profile_choice=$(choose "Choose" 1 \
    "Fast release: optimised, quick to rebuild" \
    "Debug: quickest to build, slower to run" \
    "Release: thin LTO, slowest to build")
case $profile_choice in
    Debug*)
        variant=debug profile=dev profile_dir=debug
        profile_env=()
        ;;
    Release*)
        variant=release profile=release profile_dir=release
        profile_env=(--env=CARGO_PROFILE_RELEASE_LTO=thin
            --env=CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1)
        ;;
    *)
        variant=fast profile=release profile_dir=release
        profile_env=(--env=CARGO_PROFILE_RELEASE_OPT_LEVEL=1
            --env=CARGO_PROFILE_RELEASE_INCREMENTAL=true
            --env=CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16)
        ;;
esac

if command -v flatpak >/dev/null && flatpak info org.flatpak.Builder >/dev/null 2>&1; then
    builder=(flatpak run org.flatpak.Builder)
elif command -v flatpak-builder >/dev/null; then
    builder=(flatpak-builder)
else
    echo "Install org.flatpak.Builder from Flathub, or flatpak-builder." >&2
    exit 1
fi

manifest_value() {
    python3 -c '
import json, sys
manifest = json.load(open(sys.argv[1]))
if sys.argv[2] == "finish-args":
    print("--command=" + manifest["command"])
    for arg in manifest.get("finish-args", []):
        print(arg)
elif sys.argv[2] == "cleanup":
    # The manifest-wide rules and those of every module.
    rules = list(manifest.get("cleanup", []))
    for module in manifest.get("modules", []):
        if isinstance(module, dict):
            rules += module.get("cleanup", [])
    for rule in rules:
        print(rule)
else:
    print(manifest[sys.argv[2]])
' "$MANIFEST" "$1"
}

sdk=$(manifest_value sdk)/ARCH/$(manifest_value runtime-version)

# Sets up the build directory with the SDK and its extensions, stopping
# before Ripples itself.
build_deps() {
    local arch=$1 state=.flatpak/$1
    echo "==> Preparing the SDK for $arch"
    # Missing SDKs go to whichever installation already holds this
    # architecture's SDK, and system-wide when neither does.
    # flatpak-builder only looks in the installation it is told to use.
    local ref=${sdk/ARCH/$arch} scope=()
    if ! flatpak --system info "$ref" >/dev/null 2>&1 &&
        flatpak --user info "$ref" >/dev/null 2>&1; then
        scope=(--user)
    fi
    "${builder[@]}" "${scope[@]}" --arch="$arch" --force-clean --stop-at="$MODULE" \
        --state-dir=.flatpak/builder --install-deps-from=flathub \
        "$state/build" "$MANIFEST"
    sha256sum "$MANIFEST" >"$state/manifest.sha256"
}

build_app() {
    local arch=$1 state=.flatpak/$1
    mkdir -p "$state"
    # The SDK setup is redone only when the manifest changes. The app ID is
    # checked as well, since the build directory carries it into the
    # exported ref.
    if [[ ! -f $state/build/metadata ]] ||
        ! grep -qx "name=$APP_ID" "$state/build/metadata" ||
        ! sha256sum --status -c "$state/manifest.sha256" 2>/dev/null; then
        build_deps "$arch"
    fi
    local target_dir=$state/target-$variant
    local sdk_shell=(flatpak build --share=network --filesystem="$PWD"
        --env=PATH=/app/bin:/usr/bin:/usr/lib/sdk/rust-stable/bin
        --env=CARGO_HOME="$PWD/.flatpak/cargo"
        --env=CARGO_TARGET_DIR="$PWD/$target_dir"
        --env=CARGO_TERM_COLOR=always
        "${profile_env[@]}" "$state/build")
    echo "==> Compiling $arch ($variant)"
    "${sdk_shell[@]}" cargo build --profile "$profile"
    # shellcheck disable=SC2016 # $1 expands inside the sandbox.
    "${sdk_shell[@]}" sh -c '
        set -e
        install -Dm755 "$1/'"$BINARY"'" /app/bin/'"$BINARY"'
        install -Dm644 assets/'"$APP_ID"'.desktop /app/share/applications/'"$APP_ID"'.desktop
        install -Dm644 assets/icons/app/'"$APP_ID"'.svg /app/share/icons/hicolor/scalable/apps/'"$APP_ID"'.svg
        install -Dm644 assets/icons/app/'"$APP_ID"'-symbolic.svg /app/share/icons/hicolor/symbolic/apps/'"$APP_ID"'-symbolic.svg
        if [ -f assets/'"$APP_ID"'.metainfo.xml ]; then
            install -Dm644 assets/'"$APP_ID"'.metainfo.xml /app/share/metainfo/'"$APP_ID"'.metainfo.xml
        fi
    ' sh "$PWD/$target_dir/$profile_dir"
}

make_bundle() {
    local arch=$1 bundle=$2 state=.flatpak/$1
    echo "==> Bundling $bundle"
    # build-finish can only run once on a directory, so a copy is finished
    # and the build directory stays usable for incremental builds.
    local finish=$state/finish
    rm -rf "$finish"
    cp -a --reflink=auto "$state/build" "$finish"
    # The manifest's cleanup rules, which only a complete flatpak-builder
    # run applies: "/path" is under /app, a bare pattern matches anywhere.
    local rule
    while IFS= read -r rule; do
        [[ -n $rule ]] || continue
        if [[ $rule == /* ]]; then
            # shellcheck disable=SC2086 # The rule may hold a glob.
            rm -rf "$finish"/files$rule
        else
            find "$finish/files" -name "$rule" -exec rm -rf {} +
        fi
    done < <(manifest_value cleanup)
    # Debug info makes the bundle several times larger and slow to compress.
    rm -rf "$finish"/files/lib/debug
    flatpak build "$finish" strip --strip-debug /app/bin/"$BINARY"

    local finish_args
    mapfile -t finish_args < <(manifest_value finish-args)
    flatpak build-finish "${finish_args[@]}" "$finish" >/dev/null
    flatpak build-export --arch="$arch" "$state/repo" "$finish" >/dev/null
    mkdir -p "$(dirname "$bundle")"
    flatpak build-bundle --arch="$arch" --runtime-repo="$RUNTIME_REPO" \
        "$state/repo" "$bundle" "$APP_ID"
    rm -rf "$finish"
}

for arch in "${arches[@]}"; do
    if [[ $arch != "$host_arch" ]] && ! compgen -G "/proc/sys/fs/binfmt_misc/*$arch*" >/dev/null; then
        echo "Building for $arch on $host_arch needs QEMU user emulation registered" >&2
        echo "with binfmt_misc (qemu-user-static or qemu-user-binfmt)." >&2
        exit 1
    fi
done

outputs=()
for arch in "${arches[@]}"; do
    build_app "$arch"

    install=false
    file=true
    if [[ $arch == "$host_arch" ]]; then
        [[ $target == "Flatpak file" ]] || install=true
        [[ $target == "Install only" ]] && file=false
    fi

    if $file; then
        bundle=$DIST/$APP_ID-$arch.flatpak
        make_bundle "$arch" "$bundle"
        outputs+=("$bundle")
    fi
    if $install; then
        if $file; then
            source_bundle=$bundle
        else
            source_bundle=.flatpak/$arch/install.flatpak
            make_bundle "$arch" "$source_bundle"
        fi
        echo "==> Installing $arch for this user"
        flatpak install --user --reinstall -y "$source_bundle"
        $file || rm -f "$source_bundle"
    fi
done

echo
for output in "${outputs[@]}"; do
    echo "Wrote $output"
done
