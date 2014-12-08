class PsqlEdgeValidator(object):
    def __init__(self, driver, *args, **kwargs):
        self.driver = driver

    def __call__(self, node, *args, **kwargs):
        return self.validate(node, *args, **kwargs)

    def validate(self, edge, *args, **kwargs):
        return True


class PsqlNodeValidator(object):
    def __init__(self, driver, *args, **kwargs):
        self.driver = driver

    def __call__(self, node, *args, **kwargs):
        return self.validate(node, *args, **kwargs)

    def validate(self, node, *args, **kwargs):
        return True
