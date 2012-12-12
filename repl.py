import inspect
import sys

from console_format import *

class QueueTask(object):
  def __init__(self, args=''):
    self.args = args

  @classmethod
  def help(cls):
    pass

  @classmethod
  def describe(cls):
    return ''

  def execute(self, obj):
    pass

def get_first_word(line):
  command = None
  args = ''
  if ' ' in line:
    command, args = line.split(' ', 1)
  else:
    command = line
  return command, args

class REPL(object):
  def __init__(self, prompt='>> ', command_queue=None, **kwargs):
    for key in kwargs:
      setattr(self, key, kwargs[key])
    self.set_prompt(prompt)
    self.commands = {}
    self.command_queue = command_queue

  def set_prompt(self, prompt):
    self.prompt = prompt

  def add_commands_from_module(self, module):
    for obj_name in dir(module):
      x = getattr(module, obj_name)
      if not inspect.isclass(x) or not issubclass(x, QueueTask) or x == QueueTask:
        continue
      self.commands[x.command] = x

  def process_command(self, input_line):
    command, args = get_first_word(input_line)
    
    if command == '':
      pass
    elif command == 'help':
      subcommand, args = get_first_word(args)

      if subcommand == '' or subcommand == 'commands':
        # list commands        
        all_commands = self.commands.keys()
        all_commands.sort()
        for command_str in all_commands:
          sys.stdout.write('%s: %s\n' % (bold(command_str), self.commands[command_str].describe()))
        sys.stdout.write('\nyou can also type \'help command\' for more information on that command\n')
      else:
        # help for one command
        sys.stdout.write('%s\n' % bold(subcommand))
        sys.stdout.write('%s\n' % self.commands[subcommand].describe())
        sys.stdout.write('%s\n' % self.commands[subcommand].help())

    elif command in self.commands:
      command_obj = self.commands[command](args)
      self.command_queue.put(command_obj, block=False)
      if command_obj.terminal:
        return True
    else:
      sys.stdout.write('unknown command.\ntype \'help\' for a list of commands\n')
    return False

  def loop(self):
    while True:
      try:
        sys.stdout.write(self.prompt)
        input_line = raw_input()
        if input_line == 'exit':
          break
        should_stop = self.process_command(input_line)
        if should_stop:
          break
      except KeyboardInterrupt as e:
        sys.stdout.write('\n')
        break
      except EOFError as e:
        sys.stdout.write('\n')
        break
