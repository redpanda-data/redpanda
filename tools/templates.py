#!/usr/bin/env python3


def load_from_file(path):
    with open(path, 'r') as f:
        return f.read()


def render_to_file(src, target, ctx):
    import jinja2
    loader = jinja2.FunctionLoader(load_from_file)
    env = jinja2.Environment(loader=loader)
    with open(target, 'w') as f:
        s = env.get_template(src).render(ctx)
        print(s)
        f.write(env.get_template(src).render(ctx))
