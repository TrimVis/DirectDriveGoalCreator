import click

from .trace_builder import TraceBuilder, Kind


@click.command(
    name="visualize_viz",
    help="Transforms a viz file generated by logGOPSSim to a perfetto trace file, which can then be loaded into perfetto to visualize interactions",
)
@click.argument(
    "in_file", type=click.Path(exists=True, dir_okay=False, resolve_path=True)
)
@click.argument(
    "out_file", type=click.Path(exists=False, dir_okay=False, resolve_path=True)
)
@click.option(
    "--rank-name-map",
    type=click.Path(exists=True, dir_okay=False, resolve_path=True),
    help="Json file which maps rank ids to a descriptive name",
)
@click.option(
    "--expert/--simplified",
    help="Output will either contain extra detailed information or a simplified view",
    default=False,
)
@click.option(
    "--advanced/--simplified",
    help="Output will either contain detailed information or a simplified view",
    default=False,
)
def cli(in_file, out_file, rank_name_map, expert, advanced):
    kind = Kind.ADVANCED if advanced \
        else Kind.EXPERT if expert \
        else Kind.SIMPLE

    # Build trace file from rank_name_map and the viz_in_file
    builder = TraceBuilder()
    trace = builder.kind(kind).rank_name_map(
        rank_name_map).viz_file(in_file).build()

    # Write generated trace to out_file
    trace.serialize_to_file(out_file)


if __name__ == "__main__":
    cli()
