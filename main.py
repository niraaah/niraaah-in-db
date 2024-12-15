from services.dataLoader import processDataFile
import sys

if __name__ == "__main__":
    try:
        processDataFile('recruitment_data.csv')
    except Exception as e:
        sys.exit(1)
