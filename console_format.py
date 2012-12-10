
def bold(string):
  return '\033[1m%s\033[0m' % string

foreground = {
  'black': 30,
  'red': 31,
  'green': 32,
  'yellow': 33,
  'blue': 34,
  'magenta': 35,
  'cyan': 36,
  'white': 37,
}

background = {
  'black': 40,
  'red': 41,
  'green': 42,
  'yellow': 43,
  'blue': 44,
  'magenta': 45,
  'cyan': 46,
  'white': 47,
}


def color(string, foreground_color=None, background_color=None, bold=False):  
  foreground_code = str(foreground.get(foreground_color, ''))
  background_code = str(background.get(background_color, ''))

  bold_str = '1' if bold else ''
  
  format_str = ''
  for x in (bold_str, foreground_code, background_code):
    if x != '':
      if format_str != '':
        format_str += ';'
      format_str += x

  return '\033[0%sm%s\033[0m' % (format_str, string)
