import argparse
from pathlib import Path

from .config import SimulationConfig
from .data import load_events
from .engine import Simulator
from .strategies import CrossVenueArbitrageStrategy


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--max-depth", type=int, default=10)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    config = SimulationConfig(max_book_depth=args.max_depth)
    events = load_events(args.dataset, config.max_book_depth)
    result = Simulator(config).run(events, CrossVenueArbitrageStrategy(config))
    if args.output:
        result.write(args.output)
    print(result.summary())


if __name__ == "__main__":
    main()
