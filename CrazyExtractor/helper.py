import roman


def get_total_page(s):
    total = 0
    for segment in s.split(','):
        total += get_range(segment.strip())
    return total


def get_range(s):
    range_split = s.split('-')
    if len(range_split) < 2:
        return 1
    if len(range_split[1]) == 0:
        return 1
    return get_number(range_split[1]) - get_number(range_split[0]) + 1


def get_number(num):
    if num.isdigit():
        return int(num)
    else:
        try:
            return roman.fromRoman(num.upper())
        except:
            return 0