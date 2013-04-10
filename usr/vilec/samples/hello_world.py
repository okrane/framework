
import cProfile

print('Hello World')
cProfile.run('foo()')
pycallgraph.make_dot_graph('C:/temp/test.png')
