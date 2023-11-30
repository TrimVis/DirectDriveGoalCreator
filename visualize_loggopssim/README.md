# `visualize`
### Visualization helper that can generate perfetto trace files from vis files

## About
`visualize` can create perfetto trace files from logGOPSsim .viz files (use the `-V` flag to generate) .


## Usage
You need a .viz file to visualize.
If you don't have one create one using a goal file and logGOPSsim and txt2bin:
`./txt2bin <GOAL_FILE> out.bin \
    && ./LogGOPSim -f out.bin -V out.viz`

You can now visualize this file by running:
`./visualize <VIZ_SRC> <TRACE_DEST>`

Go to the [Perfetto Web UI](https://ui.perfetto.dev/) and upload the generated trace file there for visualization.

