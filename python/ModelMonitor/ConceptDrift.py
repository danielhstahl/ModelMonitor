from pyspark.ml.wrapper import JavaTransformer
from pyspark.ml.base import Transformer
from pyspark.ml.util import _jvm

def serializeToBundle(self, dataset, columnNameAndTypeArray):
    serializer = SimpleConceptDrift()
    serializer.getDistribution(self, dataset, columnNameAndTypeArray)


def deserializeFromBundle(path):
    serializer = SimpleConceptDrift()
    return serializer.deserializeFromBundle(path)

setattr(Transformer, 'serializeToBundle', serializeToBundle)
setattr(Transformer, 'deserializeFromBundle', staticmethod(deserializeFromBundle))


class SimpleConceptDrift(object):
    def __init__(self):
        super(SimpleConceptDrift, self).__init__()
        self._java_obj = _jvm().ml.dhs.ModelMonitoring.ConceptDrift()

    def getDistribution(self, dataset, columnNameAndTypeArray):
        self._java_obj.getDistribution(dataset._jdf, columnNameAndTypeArray)

    def saveDistribution(self, distribution, path):
        self._java_obj.saveDistribution(distribution, path)

    def deserializeFromBundle(self, path):
        return JavaTransformer._from_java( self._java_obj.deserializeFromBundle(path))