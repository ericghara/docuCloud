from random import randint
from os import path

# This produces 2 CSVs that are designed to be used with TestFileTree and TestFiles
# classes in (the java project) to generate a test database.  This is guaranteed
# not to produce any duplicate edges and the degree of each node is guaranteed
# to be >= 1.  The distribution of edges is nontrivial
# to characterize, smaller numbered nodes will have significantly larger degrees
# than larger.  The delta b/t the number of objects and number of resources should
# follow a normal distribution (i.e. expect a roughly equal number of nodes for each).

class RandomDocuCloudRecords:

    TREE_OBJ_PREFIX = "fileObj"
    FILE_RES_PREFIX = "fileRes"
    FILENAME_SUFFIX_TREE = "_edge_tree_objects.csv"
    FILENAME_SUFFIX_FILE = "_edge_file_resources.csv"
    TREE_ROOT_ENUM = "ROOT"
    TREE_ROOT_PATH = ""
    TREE_FILE_ENUM = "FILE"

    def _createEdges(self, num_edges):
        adjacencies = [None] * num_edges
        seen = set()
        num_obj = 0
        num_res = 0
        i = 0
        while i < num_edges:
            obj_random = randint(0, num_obj)
            res_random = randint(0, num_res)
            adj = (obj_random,res_random)
            if adj not in seen:
                seen.add(adj)
                adjacencies[i] = (obj_random, res_random)
                num_obj = max(obj_random + 1, num_obj)
                num_res = max(res_random + 1, num_res)
                i +=1
        adjacencies.sort()
        return num_obj, self._createFileResCsv(adjacencies)

    def _createFileResCsv(self, adjacencies: list[list[int]]) -> list[str]:
        return [f"{self.TREE_OBJ_PREFIX}{t},{self.FILE_RES_PREFIX}{f}\n" for t, f in adjacencies]


    def _createTreeObjects(self, num_obj):
        line_template = lambda obj_type, path: f'{obj_type},"{path}"\n'

        tree_obj_lines = [None] * (num_obj + 1)
        tree_obj_lines[0] = line_template(self.TREE_ROOT_ENUM,self.TREE_ROOT_PATH)
        for i in range(1, len(tree_obj_lines) ):
            tree_obj_lines[i] = line_template(self.TREE_FILE_ENUM, self.TREE_OBJ_PREFIX + str(i-1) )
        return tree_obj_lines

    def _writeOut(self, save_path, lines):
        with open(save_path, 'x') as fp:
            for line in lines:
                fp.write(line)

    def create(self, num_edges, save_path):
        num_obj, file_res_lines = self._createEdges(num_edges)
        tree_obj_lines = self._createTreeObjects(num_obj)
        file_res_filename = f"{num_edges}{self.FILENAME_SUFFIX_FILE}"
        tree_obj_filename = f"{num_edges}{self.FILENAME_SUFFIX_TREE}"
        self._writeOut(path.join(save_path, file_res_filename), file_res_lines)
        self._writeOut(path.join(save_path, tree_obj_filename), tree_obj_lines)


NUM_EDGES = 50_000
SAVE_PATH = r"/mnt/code/java/docu-cloud/backend/src/test/resources/"
randomRecords = RandomDocuCloudRecords()
randomRecords.create(NUM_EDGES, SAVE_PATH)
