# Go MAD

[![CircleCI](https://circleci.com/gh/circleci/circleci-docs.svg?style=shield)](https://circleci.com/gh/doutorfinancas/go-mad)
[![codecov](https://codecov.io/gh/doutorfinancas/go-mad/branch/master/graph/badge.svg?token=L5D1OP1229)](https://codecov.io/gh/doutorfinancas/go-mad)
## MySQL anonymized dump
This project aims to have a tool that allows mysql repositories to be quickly dump in an anonymized form.

I have found numerous projects that strive to do somehow the same, but none gave me the tooling that would fit my 
requirements, which are, have a sort of faker and fill the mysql dump data with them

Notorious Projects that could do similar:
- [mysqlsuperdump](https://github.com/hgfischer/mysqlsuperdump), but hasn't had updates since 2017
- [mtk-dump](https://github.com/skpr/mtk) based on the previous, anonymization via query

Usage:

from shell, call:
```shell
go-mad dump my_database --config=config_example.yml
```

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
- [ ] Feel free to expand this list
