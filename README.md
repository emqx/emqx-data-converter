# EMQX Enterprise v4 Data Migration

A CLI tool to convert EMQX 4.4 JSON backup files to EMQX v5 and v6 archive files.
The converted .tar.gz file can be imported to EMQX Enterprise 5.6.1 or newer (including v6).

Currently, the following configs/data are converted:

- Builtin (Mnesia) authn/authz (both data and config)
- Internal (file) authz
- Redis authn/authz config
- MySQL authn/authz config
- PostgreSQL authn/authz config
- HTTP authn/authz config
- MongoDB authn/authz config
- JWT authn config
- PSK authentication data
- Blacklist (banned clients)
- API Keys (Applications)
- Rules and resources(bridges), except for:
  - IoTDB
  - Consumer bridges (modules in EMQX 4.4)
    + MQTT Consumer has been supported.

Incompatibilities that cannot be handled by the converter automatically are printed to the stdout,
please don't ignore these warnings, as they may be helpful to correct compatibility issues afterwards.

# Quick start GNU/Linux

Download `emqx_data_converter.tar.gz` from the latest GitHub release and unpack it.
The converter is ready to be used, no dependencies need to be installed (verified on Ubuntu 20.04, 22.04, Debian 12).

```shell
tar -xf emqx_data_converter.tar.gz
emqx_data_converter/bin/emqx_data_converter <input file path>
```

# Quick start Docker

To run using a ready Docker image, you may use the following command, replacing paths accordingly:

```sh
docker run --rm -it \
  -v /path/to/backup.json:/mnt/backup.json \
  -v /path/where/to/place/output:/output \
  -v /path/to/emqx4/data/dir:/emqx4data \
  ghcr.io/emqx/emqx-data-converter:latest \
    -o /output \
    -r /emqx4data \
    /mnt/backup.json
```

More usage info may be obtained with:

```sh
docker run --rm -it ghcr.io/emqx/emqx-data-converter:latest
```

# Usage

Basic usage:

```shell
emqx_data_converter <input file path>
```

where `<input file path>` is a path to  EMQX 4.4 backup JSON file.
This will produce EMQX 5.6.1+ backup file in the current working directory,
the output file name will be printed out to the console:

```
[INFO] Converted to EMQX 5.6.1+ backup file: /conv/emqx-export-2023-07-13-14-48-58.895.tar.gz
```

Output directory can be set with `-o <dir path>` or `--output-dir <dir path>` option.
Please note that the converter always needs write access to the current working directory (even when `-o <output dir>` is used),
because it uses to create some temporary files.

Version:

```shell
emqx_data_converter -v
emqx_data_converter --version
```

Help:

```shell
emqx_data_converter -h
emqx_data_converter --help
```

Help command contains info about several 'business logic' options, please make sure to read it.

## Importing converted data

After convertion, the output tarball contains a structure similar to this:

```
/tmp/emqx-export-2024-10-10-14-27-58.171/
├── cluster.hocon
├── META.hocon
└── mnesia
    ├── emqx_acl
    ├── emqx_app
    ├── emqx_authn_mnesia
    ├── emqx_banned
    ├── emqx_psk
    └── emqx_retainer_message
```

> [!TIP]
> There's no need to extract this file.  Just place it somewhere where `emqx` application
> user may read it.

- To import this file, simply run:

  ```sh
  emqx ctl data import full/path/to/emqx-export-2024-10-10-14-27-58.171.tar.gz
  ```

- If you have retained messages being imported (from `mnesia/emqx_retainer_message`), then, after importing that table with the command above, you must run:

  ```sh
  emqx ctl retainer reindex start
  ```

# Build

## Prerequisites

Install the following dependencies:

- Erlang/OTP 25+
- Rebar3
- Docker (optional)

## Compile emqx-data-converter

```
rebar3 escriptize
```

## Build and run in Docker

1. Clone the repository, or you can also download the source code as a zip file and extract it.

```shell
git clone https://github.com/emqx/emqx-data-converter.git
```

2. Copy the EMQX 4.4 backup files to the `input` directory.

```shell
cd emqx-data-converter
mkdir input
cp <path-to-emqx-4.4-backup-files-dir>/emqx-export-2023-7-13-15-52-15.json <path-to-emqx-4.4-backup-files-dir>/emqx_retainer.DCD input/
```

Where `<path-to-emqx-4.4-backup-files-dir>` is the path to the directory where EMQX 4.4 backup JSON files are stored. It is assumed that we have a JSON backup file named `emqx-export-2023-7-13-15-52-15.json` and a data backup file named `emqx_retainer.DCD`.

3. Start a `erlang:25` container and compile the `escript` in it.

```shell
docker run --rm -it -v $(pwd):/emqx-data-converter -v $(pwd)/input:/input erlang:25 bash

cd /emqx-data-converter
# Ignore the warning about unsafe directory
git config --global --add safe.directory '*'
rebar3 escriptize
_build/default/bin/emqx_data_converter --data-files-dir /input /input/emqx-export-2023-7-13-15-52-15.json
```

## Running tests

To run the test suite locally, first compile the escript and copy it to the project root.

```sh
rebar3 escriptize
cp _build/default/bin/emqx_data_converter ./
```

Then, run the script:

```sh
source test/scripts/env.sh
test/scripts/test-convert-and-load.exs
```

If you want to run only tests with a specific tag, for example, only those tagged with `bridges` (i.e., those that have `@tag :bridges` above them):

```sh
test/scripts/test-convert-and-load.exs --only bridges
```

# Bundle the `escript` with Erlang/OTP

```shell
./package
```

`package` script will produce `_build/emqx_data_converter.tar.gz.` It is the `escript` bundled with Erlang/OTP,
which can be copied to and used on another machine without the need to install Erlang/OTP and Rebar3.
Note: the target machine OS must match the OS on which the package was created, otherwise it won't work.
