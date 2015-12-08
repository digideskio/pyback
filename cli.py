import os
import pprint
import sys
import time

def get_console_dimensions():
    return os.popen('stty size', 'r').read().split()

def format_timespan(seconds):
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return "%d:%02d:%02d" % (h, m, s)


def format_filesize(bytes, decimal_digits=1):
    format_str = "%." + str(decimal_digits) + "f %sB"
    for unit in ['','K','M','G','T','P','E','Z']:
        if abs(bytes) < 1024.0:
            return format_str % (bytes, unit)
        bytes /= 1024.0
    return format_str % (bytes, 'Y')


def cli_progress(filename,
                 formattedFileSize,
                 formattedPartSize,
                 startTime,
                 current_val,
                 end_val,
                 bar_length=20):

    rows, columns = get_console_dimensions()

    percent = float(current_val) / end_val
    hashes = '#' * int(round(percent * bar_length))
    spaces = ' ' * (bar_length - len(hashes))
    rate, remaining = get_rate_and_remaining(startTime, current_val, end_val)

    if current_val == 0:
        output = "\r[%s] %s [%s/%s] (%s parts)" % (
            hashes + spaces,
            filename,
            format_filesize(current_val),
            formattedFileSize,
            formattedPartSize)
    elif current_val == end_val:
        output = "\r[%s] %s %d%% [%s] (%s parts at %s) %s" % (
            hashes + spaces,
            filename,
            int(round(percent * 100)),
            formattedFileSize,
            formattedPartSize,
            rate,
            remaining)
    else:
        output = "\r[%s] %s %d%% [%s/%s] (%s parts at %s) %s" % (
            hashes + spaces,
            filename,
            int(round(percent * 100)),
            format_filesize(current_val),
            formattedFileSize,
            formattedPartSize,
            rate,
            remaining)

    sys.stdout.write(output.ljust(int(columns)))
    if current_val == end_val:
        sys.stdout.write('\n')
    sys.stdout.flush()


def pp(data):
    rows, columns = get_console_dimensions()
    pp = pprint.PrettyPrinter(indent=4, width=columns)
    pp.pprint(data)


def get_rate_and_remaining(startTime, val, end_val):
    elapsed = time.time() - startTime
    remaining = ""
    if elapsed:
        rate = val / elapsed
        if end_val - val > 0:
            if rate > 0:
                remaining = format_timespan((end_val - val) / rate)
            else:
                remaining = "..."
        else:
            remaining = format_timespan(elapsed)
    else:
        rate = 0
    return ("%s/sec" % (format_filesize(rate, 2)), remaining)
