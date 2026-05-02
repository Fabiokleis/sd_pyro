# sd_pyro

trabalho para disciplina de sistemas distribuídos, uma implementação do algoritmo de consenso distribuído **Raft** em Python, utilizando **Pyro5** para a comunicação via RPC entre os nós.

## tools

O projeto foi construido com as seguintes ferramentas do ecossistema python:

* **[NixOS (shell.nix)](https://nixos.org/)**
* **[Pyro5](https://pyro5.readthedocs.io/)**
* **[uv](https://github.com/astral-sh/uv)**
* **[Ruff](https://docs.astral.sh/ruff/)**
* **[Mypy](https://mypy.readthedocs.io/)**
* **[Pytest](https://docs.pytest.org/)**

## setup
para instalar as dependencias utilize o uv, uma vez ja no shell do nix:
```bash
uv sync
```

## lint
para checagem de padrao de codigo com ruff: 
```bash
uv run ruff format
```

```bash
uv run ruff check --fix
```

## types
para checagem de tipos:
```bash
uv run mypy .
```

## test
suite de testes python:
```bash
uv run pytest
```


## run
```bash
uv run main.py
```
