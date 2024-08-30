import argparse
import datetime
import re
from collections import defaultdict
from logging import getLevelNamesMapping, getLogger
from operator import itemgetter

_logger = getLogger(__name__)


LOG_LEVELS = tuple(getLevelNamesMapping())
REQUEST_KEY = "REQUEST"

_YEAR_SUFFIX = "202"
_LEVEL_MAPPING = getLevelNamesMapping()

__version__ = "0.0.1"
__author__ = "Pouya MN"
def read_log(logfile, threshold=None, severity=None, ignore_no_db=True):
    """
    read log file
    :param logfile: pathlike object to the log file
    :param threshold: minimum request total time to cache, in seconds. not capturing if not provided
    :param severity: minimum severity to cache for non-requests logs, not capturing if not provided
    :param ignore_no_db: ignore logs without database
    :return: dict(list(dict)) log data, separated by log level or 'request'
    """
    if not threshold and severity is None:
        return {}

    threshold = threshold or 0
    if severity is None:
        severity = 1 + max(_LEVEL_MAPPING.values())
    log = defaultdict(list)
    current_line = {}
    with open(logfile, "r") as log_file:
        total = log_file.seek(0, 2)
        log_file.seek(0)
        pos = 0
        for line in log_file:

            if _logger.isEnabledFor(_LEVEL_MAPPING["DEBUG"]):
                if pos % 10000 == 0:
                    print(f"\r{str(int(100 * pos / total))}%", end=" ")
                pos += len(line)

            # check if any of LOG_LEVELS is in the line
            if line.startswith(_YEAR_SUFFIX) and any(
                f" {ll} " in line[:40] for ll in LOG_LEVELS
            ):
                # end previous line
                if current_line:
                    log[current_line["level"]].append(current_line)
                current_line = {}

                fragments = line.split()
                level = fragments[3]
                logger = fragments[5][:-1]
                db = fragments[4]
                if ignore_no_db and db == "?":
                    continue
                if logger == "werkzeug" and level == "INFO":
                    # request log, fixed format
                    if not threshold or line.endswith("-"):
                        # no useful perf data, just skip the line
                        continue
                    other_time = float(fragments[-1])
                    sql_time = float(fragments[-2])
                    total_time = other_time + sql_time
                    if total_time < threshold:
                        continue
                    current_line["ip"] = fragments[6]
                    current_line["method"] = fragments[11][1:]
                    current_line["endpoint"] = fragments[12]
                    current_line["status"] = fragments[14]
                    current_line["other_time"] = other_time
                    current_line["sql_time"] = sql_time
                    current_line["total_time"] = total_time
                    current_line["query_count"] = int(fragments[-3])
                    current_line["level"] = REQUEST_KEY

                else:
                    if _LEVEL_MAPPING.get(level, 0) < severity:
                        continue
                    current_line["message"] = " ".join(fragments[6:])
                    current_line["context"] = []

                date_str = fragments[0]
                time_str = (
                    fragments[1].split(".")[0].split(",")[0]
                )  # remove milliseconds
                current_line["timestamp"] = datetime.datetime.strptime(
                    " ".join([date_str, time_str]), "%Y-%m-%d %H:%M:%S"
                )
                current_line["pid"] = int(fragments[2])
                current_line.setdefault("level", level)
                # we can consider not adding db, or keep a separate log per db instead
                current_line["db"] = db
                current_line["logger"] = logger
            else:
                if "context" in current_line:
                    current_line["context"].append(line)
    if _logger.isEnabledFor(_LEVEL_MAPPING["DEBUG"]):
        print("\r      \r", end="")
    return log


def _print_table(title, lines, field, value):
    max_len_field = max(len(str(line[field])) for line in lines)
    max_len_value = max(len(str(line[value])) for line in lines)
    max_len = max_len_field + max_len_value + 7
    print("=" * max_len)
    print(f"|{title:^{max_len -2}}|")
    print("-" * max_len)
    for line in lines:
        print(f"| {line[field]:<{max_len_field}} | {line[value]:<{max_len_value}} |")
    print("-" * max_len)
    print()


def main():
    Parser = argparse.ArgumentParser()
    Parser.add_argument("logfile", type=str)
    Parser.add_argument("-t", "--threshold", type=int, default=50)
    Parser.add_argument("-s", "--severity", choices=LOG_LEVELS, default="INFO")
    Parser.add_argument("-i", "--include-no-db", action="store_true")
    Parser.add_argument("-R", "--no-requests", action="store_true")
    Parser.add_argument("-r", "--just-requests", action="store_true")
    Parser.add_argument("-q", "--quiet", action="store_true")
    Parser.add_argument("-v", "--vervose", action="store_true")
    Args = Parser.parse_args()
    if Args.no_requests:
        Args.threshold = 0
    if Args.just_requests:
        Args.severity = None

    if Args.vervose:
        _logger.setLevel("DEBUG")
    if Args.quiet:
        _logger.setLevel("WARNING")

    logs = read_log(
        Args.logfile,
        Args.threshold,
        _LEVEL_MAPPING.get(Args.severity, 1000),
        not Args.include_no_db,
    )
    print("\n".join(f"{level}: {len(lines)}" for level, lines in logs.items()))
    if not Args.no_requests:
        requests = logs[REQUEST_KEY]
        lines = sorted(requests, key=itemgetter("total_time"), reverse=True)[:10]
        _print_table("Top 10 Slow requests", lines, "endpoint", "total_time")

        ep_hits = defaultdict(int)
        ep_times = defaultdict(int)
        for line in requests:
            ep = line["endpoint"]
            # ignore languages in frontent urls
            ep = re.sub(r"^/[a-z]{2}_[A-Z]{2}/", "/", ep)
            ep_times[ep] = (ep_times[ep] * ep_hits[ep] + line["total_time"]) / (
                ep_hits[ep] + 1
            )
            ep_hits[ep] += 1

        lines = [
            {"endpoint": endpoint, "time": f"{time:.1f}"}
            for endpoint, time in sorted(
                ep_times.items(), key=itemgetter(1), reverse=True
            )[:10]
        ]
        _print_table("Top 10 slow endpoints", lines, "endpoint", "time")

        lines = [
            {"endpoint": endpoint, "hits": hits}
            for endpoint, hits in sorted(
                ep_hits.items(), key=itemgetter(1), reverse=True
            )[:10]
        ]
        _print_table("Top 10 most hit endpoints:", lines, "endpoint", "hits")


if __name__ == "__main__":
    main()
