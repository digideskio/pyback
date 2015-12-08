import os
import pprint
import sys
import time

rows, columns = os.popen('stty size', 'r').read().split()

def format_timespan(seconds):
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return "%d:%02d:%02d" % (h, m, s)


def cli_progress(filename, startTime, current_val, end_val, bar_length=20):

    percent = float(current_val) / end_val
    hashes = '#' * int(round(percent * bar_length))
    spaces = ' ' * (bar_length - len(hashes))
    rate, remaining = get_rate_and_remaining(startTime, current_val, end_val)

    output = "\r[{0}] {1} {2}% ({3}) {4}".format(
        hashes + spaces, filename, int(round(percent * 100)), rate, remaining)
    sys.stdout.write(output.ljust(int(columns)))

    if current_val == end_val:
        sys.stdout.write('\n')
    sys.stdout.flush()


def pp(data):
    pp = pprint.PrettyPrinter(indent=4, width=columns)
    pp.pprint(data)


def get_rate_and_remaining(startTime, val, end_val):
    elapsed = time.time() - startTime
    remaining = ""
    if elapsed:
        rate = val / elapsed
        if end_val - val > 0:
            remaining = format_timespan((end_val - val) / rate)
        else:
            remaining = format_timespan(elapsed)
    else:
        rate = 0
    return ("{0:.2f} KB/sec".format(rate/1024), remaining)
