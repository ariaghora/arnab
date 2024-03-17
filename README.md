<h1 align="center">A R N A B</h1>

<p align="center">
  <img src="assets/arnab.png" width=200/>
</p>

<h4 align="center">
Aria's cheap man's experimental data transformation tool, built on top of DuckDB 🦆
</h4>

Arnab aims to offer a simple, cross-platform solution for organizing and executing data models, focusing on core functionalities essential for data workflows.
Just like [DBT](https://docs.getdbt.com/), but is with subset of its features, jinja-less (except for macros), and (theoretically) more portable.

Its main purpose is simple: give it a directory containing SQL files, it will scan the directory recursively and determine the order of execution of all queries.

[![Arnab demo 1](https://img.youtube.com/vi/CwmvRuyRLy8/0.jpg)](https://www.youtube.com/watch?v=CwmvRuyRLy8)


## Getting Started

Create a project directory where you want to put all of your SQL files.
Create `config.yaml` at the root of the directory.
At the very least, it should contain `models_dir` and `db_path` information:

```yaml
models_dir: .
db_path: data.duckdb
```

Optionally, you can create another directories (that may contain subdirectories) and use its name for `models_dir`.

Check examples directory for more.

### Running pipeline

Set the working directory to the root of your project, then run `arnab run`.

### Visualizing pipeline

We can get the visualization of the pipeline in a SVG file format for an additional way to debug the pipeline.
Set the working directory to the root of your project, then run `arnab viz outout_name.svg`.

## Features

- [x] Single executable file
- [x] Cross-platform
- [x] Automatically determine the order of model execution
- [x] Model materialization types:
    - [x] Table
    - [x] View
    - [ ] ~~Incremental~~ (probably not necessary for now)
- [x] Macro
