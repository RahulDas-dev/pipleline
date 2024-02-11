import logging

from joblib import Parallel, delayed, parallel_config

from pipeline import Data, PipeLine

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
    config_path = "./config.cfg"
    pipeline = PipeLine.load(config_path)

    print(pipeline)
    data_ = [
        Data(identifier="1", metadata={}, obj=[1, 2, 2, 3, 4, 5, 6, 7, 8, 9]),
        Data(identifier="2", metadata={}, obj=[1, 2, 2, 3, 4, 5, 6, 7, 8, 9]),
        Data(identifier="3", metadata={}, obj=[1, 2, 2, 3, 4, 5, 6, 7, 8, 9]),
        Data(identifier="4", metadata={}, obj=[1, 2, 2, 3, 4, 5, 6, 7, 8, 9]),
        Data(identifier="5", metadata={}, obj=[1, 2, 2, 3, 4, 5, 6, 7, 8, 9]),
        Data(identifier="6", metadata={}, obj=[1, 2, 2, 3, 4, 5, 6, 7, 8, 9]),
        Data(identifier="7", metadata={}, obj=[1, 2, 2, 3, 4, 5, 6, 7, 8, 9]),
        Data(identifier="8", metadata={}, obj=[1, 2, 2, 3, 4, 5, 6, 7, 8, 9]),
        Data(identifier="9", metadata={}, obj=[1, 2, 2, 3, 4, 5, 6, 7, 8, 9]),
        Data(identifier="10", metadata={}, obj=[1, 2, 2, 3, 4, 5, 6, 7, 8, 9]),
        Data(identifier="11", metadata={}, obj=[1, 2, 2, 3, 4, 5, 6, 7, 8, 9]),
        Data(identifier="12", metadata={}, obj=[1, 2, 2, 3, 4, 5, 6, 7, 8, 9]),
    ]
    results = pipeline(data_)
    print(results)
