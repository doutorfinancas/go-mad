# Go MAD

[![Latest Release](https://img.shields.io/github/v/release/doutorfinancas/go-mad)](https://github.com/doutorfinancas/go-mad/releases)
[![CircleCI](https://circleci.com/gh/circleci/circleci-docs.svg?style=shield)](https://circleci.com/gh/doutorfinancas/go-mad)
[![codecov](https://codecov.io/gh/doutorfinancas/go-mad/branch/master/graph/badge.svg?token=L5D1OP1229)](https://codecov.io/gh/doutorfinancas/go-mad)
[![Github Actions](https://github.com/doutorfinancas/go-mad/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/doutorfinancas/go-mad/actions)
[![APACHE-2.0 License](https://img.shields.io/github/license/doutorfinancas/go-mad)](LICENSE)

## MySQL anonymized dump
This project aims to have a tool that allows mysql repositories to be quickly dump in an anonymized form.

![what not to do](img/uh_no.png)

CommitStrip Illustration - [we all do it, right?](https://www.commitstrip.com/en/2021/12/07/we-all-do-it-right/)

Since we shouldn't, never ever, directly duplicate a production db to QA, testing or dev; this way we can safely perform that regression or performance test.

I have found numerous projects that strive to do somehow the same, but none gave me the tooling that would fit my 
requirements, which are, have a sort of faker and fill the mysql dump data with them

Notorious Projects that could do similar:
- [mysqlsuperdump](https://github.com/hgfischer/mysqlsuperdump), but hasn't had updates since 2017
- [mtk-dump](https://github.com/skpr/mtk) based on the previous, anonymization via query

## Installation

Currently, a version is still not available due to the fact this repo is a work in progress
but will soon be available (a few days)

```shell
go install github.com/doutorfinancas/go-mad@0.1.0
```

## Usage

from shell, call:
```shell
go-mad dump my_database --config=config_example.yml
```

if you are using innodb, then the configurations recommended are:
```shell
go-mad dump my_database --config=config_example.yml --single-transaction --quick
```

It will create a larger dump, but wraps everything around a transaction, disables locks and dumps writes faster
To reduce insert impact, you can replace the `--quick` with `--insert-into-limit=10` or whichever limit size would be 
best for you.

The database argument is required. Currently, only exporting one database is supported

you can use either SQL direct commands or faker on rewrites. Else it's compatible with mtk-dump config

please refer to faker documentation [here](https://pkg.go.dev/github.com/jaswdr/faker)

## Available Flags (all are optional)

| Flag (short)                   | Description                                                                                 | Type    |
|--------------------------------|---------------------------------------------------------------------------------------------|---------|
| --host (-h)                    | your MySQL host, default `127.0.0.1`                                                        | string  |
| --user (-u)                    | your user to authenticate in mysql, no default                                              | string  |
| --password (-p)                | password to authenticate in mysql, no default                                               | string  |
| --port (-P)                    | port to your mysql installation, default `3306`                                             | string  |
| --config (-c)                  | path to your go-mad config file, example below                                              | string  |
| --output (-o)                  | path to the intended output file, default STDOUT                                            | string  |
| --char-set                     | uses SET NAMES command with provided charset, default utf8                                  | string  |
| --insert-into-limit            | defines limit to be used with each insert statement, cannot use with --quick, default `100` | int     |
| --debug (-v)                   | turns on verbose mode if passed                                                             | bool    |
| --quiet (-q                    | disables log output if passed                                                               | bool    |
| --skip-lock-tables             | skips locking mysql tables when dumping                                                     | bool    |
| --single-transaction           | does the dump within a single transaction by issuing a BEGIN Command                        | bool    |
| --quick                        | dump writes row by row as opposed to using extended inserts                                 | bool    |
| --add-locks                    | add write lock statements to the dump                                                       | bool    |


## Configuration Example
```yaml
rewrite:
  users:
    email: faker.Internet().Email()
    password: '"FAKE_PASSWORD"'
    username: faker.Internet().Email()
    # name: faker.Person().Name()
    name: "SELECT names FROM random WHERE id = users.id"

nodata:
  - actions
  - exports
  - tokens

ignore:
  - advertisers
  - transactions
  - cache

where:
  users: |-
    id < 5000
```

## Next Steps (ToDos)
- [ ] Adds support to exporting multiple databases at a time
- [ ] Exports run in goroutines to accelerate when `--parallel` is passed
- [ ] Add support for env vars
- [ ] Feel free to expand this list
