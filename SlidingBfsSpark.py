from pyspark import SparkContext
import Sliding, argparse

def bfs_map(value):
    """ YOUR CODE HERE """
    mapVal = []
    mapVal.append((value[0], value[1]))
    if value[1] == level:
        pos = Sliding.hash_to_board(WIDTH, HEIGHT, value[0])
        for cpos in Sliding.children(WIDTH, HEIGHT, pos):
            cpos2 = Sliding.board_to_hash(WIDTH, HEIGHT, cpos)
            mapVal.append((cpos2,level+1))
    return mapVal


def bfs_reduce(value1, value2):
    """ YOUR CODE HERE """
    return min(value1, value2)


def solve_puzzle(master, output, height, width, slaves):
    global HEIGHT, WIDTH, level, frontRDD
    HEIGHT=height
    WIDTH=width
    level = 0
    sc = SparkContext(master, "python")

    """ YOUR CODE HERE """

    # The solution configuration for this sliding puzzle. You will begin exploring the tree from this node
    sol = Sliding.solution(WIDTH, HEIGHT)
    print sol

    """ YOUR MAP REDUCE PROCESSING CODE HERE """
    max_level = 6
    frontRDD = sc.parallelize([(Sliding.board_to_hash(WIDTH, HEIGHT, sol), level)])
    count = 0

    prevCount = -1
    while 1:
        if count % 8 == 0:
           frontRDD = frontRDD.repartition(PARTITION_COUNT)
        frontRDD = frontRDD.flatMap(bfs_map)
        frontRDD = frontRDD.reduceByKey(bfs_reduce)
        count = count + 1
        level = level + 1
        currCount = frontRDD.count()
        if currCount > prevCount:
            prevCount = currCount
        else:
            break
    #frontRDD = frontRDD.map(lambda x: (x[1], x[0]))
    #frontRDD = frontRDD.sortByKey(True, PARTITION_COUNT)
    """ YOUR OUTPUT CODE HERE """
    frontRDD.coalesce(slaves).saveAsTextFile(output)
    #outList = frontRDD.collect()
    #for l in outList:
        #output(str(l[0])+" "+str(l[1]))

    sc.stop()



""" DO NOT EDIT PAST THIS LINE

You are welcome to read through the following code, but you
do not need to worry about understanding it.
"""

def main():
    """
    Parses command line arguments and runs the solver appropriately.
    If nothing is passed in, the default values are used.
    """
    parser = argparse.ArgumentParser(
            description="Returns back the entire solution graph.")
    parser.add_argument("-M", "--master", type=str, default="local[8]",
            help="url of the master for this job")
    parser.add_argument("-O", "--output", type=str, default="solution-out",
            help="name of the output file")
    parser.add_argument("-H", "--height", type=int, default=2,
            help="height of the puzzle")
    parser.add_argument("-W", "--width", type=int, default=2,
            help="width of the puzzle")
    parser.add_argument("-S", "--slaves", type=int, default=6,
            help="number of slaves executing the job")
    args = parser.parse_args()

    global PARTITION_COUNT
    PARTITION_COUNT = args.slaves * 16

    # call the puzzle solver
    solve_puzzle(args.master, args.output, args.height, args.width, args.slaves)

# begin execution if we are running this file directly
if __name__ == "__main__":
    main()
