# DirectDriveSim
### Simulating & Visualizing IO operations in an Attached Storage Network

## `trace2goal`
Tool to allow the translation of DirectDrive IO interactions to network dependencies and actions specified in the GOAL file format.
The generated goal files can later be used in logGOPSsim or htSim to simulate the actual execution.

### Usage

#### From uMass Trace
Natively the parsing of uMass[^1] storage trace files is supported.
If you have a trace file that is not in the uMass csv format, check out their PDF[^2] describing the fields and make sure to create a csv in that format first.

To transform your trace file to a goal file now simply run:
`./trace2goal trace <TRACE_SRC> <GOAL_DST>`

For more information on possible configuration check out the help page:
`./trace2goal trace --help`

#### Simple Example IO
If you simply want to create various random read and writes, check out `./trace2goal simple <GOAL_DST>`

E.g. to create a single write from one host to the disk run `./trace2goal simple --no-mount --host-count 1 --reads 0 --writes 1 <GOAL_DST>`

For more information on possible configuration check out the help page:
`./trace2goal simple --help`

#### Custom Logic
It is possible to create custom instruction using python and this module:
```python
from trace_to_goal.network import NetworkTopology, DirectDriveNetwork

topology = NetworkTopology(host_count=1, ccs_count=1, bss_count=1)
network = DirectDriveNetwork(topology=topology, slice_size=1024, disk_size=2048)

network.add_mount(0)
network.add_read(0, 0, 1024)
network.add_write(0, 0, 1024)
network.to_goal("./out.goal")
```

Check our the `cli_cs` function in `trace_to_goal/__main__.py` to see a detailed example of how to inject and create own custom DirectDrive IO interactions from python code.

### The issue of large files
Due to the nature of the goal files, the generated output goal file and intermediate files generated can be quite large.
If you experience a OS Level 'No space left on device' error, and your `df -h` reports that your /tmp partition is full, resize the tmp filesystem accordingly using:
`sudo mount -o remount,size=60G /tmp/`
(Beware: for large traces 60G might not be enough)

### Resources
[^1]: [uMass Site](https://traces.cs.umass.edu/index.php/storage/storage)
[^2]: [uMass CSV Spec](https://skulddata.cs.umass.edu/traces/storage/SPC-Traces.pdf)

## `visualize`

`visualize` can create perfetto trace files from logGOPSsim .viz files (use the `-V` flag to generate)

### Usage
You need a .viz file to visualize.
If you don't have one create one using a goal file and logGOPSsim and txt2bin:
`./txt2bin <GOAL_FILE> out.bin \
    && ./LogGOPSim -f out.bin -V out.viz`

You can now visualize this file by running:
`./visualize <VIZ_SRC> <TRACE_DEST>`

Go to the [Perfetto Web UI](https://ui.perfetto.dev/) and upload the generated trace file there for visualization.

### Adapted perfetto version
There is an adapted perfetto version provided, that allows the highlighting of all
flows at once (which is otherwise not possible)

To use it run: `./utils/host_customized_perfetto.sh`
You can then start a spec

