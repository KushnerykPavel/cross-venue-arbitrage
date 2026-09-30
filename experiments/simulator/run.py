import argparse
from pathlib import Path

from .config import SimulationConfig
from .data import load_events
from .engine import Simulator
from .splits import chronological_splits
from .strategies import CrossVenueArbitrageStrategy


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--max-depth", type=int, default=10)
    parser.add_argument("--split", choices=("train", "validation", "test"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    config = SimulationConfig(max_book_depth=args.max_depth)
    selected = None
    if args.split:
        selected = next(split for split in chronological_splits(args.dataset) if split.name == args.split)
        print(f"{selected.name}: capture_sequence [{selected.sequence_start}, {selected.sequence_end or 'end'})")
    events = load_events(
        args.dataset,
        config.max_book_depth,
        sequence_start=selected.sequence_start if selected else None,
        sequence_end=selected.sequence_end if selected else None,
    )
    result = Simulator(config).run(events, CrossVenueArbitrageStrategy(config))
    if args.output:
        result.write(args.output)
    print(result.summary())


if __name__ == "__main__":
    main()
