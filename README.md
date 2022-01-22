# Go MAD

[![CircleCI](https://circleci.com/gh/circleci/circleci-docs.svg?style=svg)](https://circleci.com/gh/doutorfinancas/go-mad)
[![codecov](https://codecov.io/gh/doutorfinancas/go-mad/branch/master/graph/badge.svg?token=L5D1OP1229)](https://codecov.io/gh/doutorfinancas/go-mad)
## MySQL anonymized dump
This project aims to have a tool that allows mysql repositories to be quickly dump in an anonymized form.

I have found numerous projects that strive to do somehow the same, but none gave me the tooling that would fit my 
requirements, which are, have a sort of faker and fill the mysql dump data with them

Notorious Projects that could do similar:
- [mysqlsuperdump](https://github.com/hgfischer/mysqlsuperdump), but hasn't had updates since 2017
- [mtk-dump](https://github.com/skpr/mtk) based on the previous, anonymization via query

Usage:

you can use either SQL direct commands or faker on rewrites. Else it's compatible with mtk-dump config

please refer to faker documentation [here](https://pkg.go.dev/github.com/jaswdr/faker)

## Example
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
