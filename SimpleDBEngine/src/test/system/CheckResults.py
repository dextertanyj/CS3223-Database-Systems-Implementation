import re
import sys

def main():
  result1 = []
  result2 = []
  with open('result.txt') as f1:
    lines = f1.readlines()
    newlines = [i.strip() for i in lines]
    result1 = newlines

  with open('answer.txt') as f2:
    lines = f2.readlines()
    newlines = [i.strip() for i in lines]
    result2 = newlines
  
  # remove and check header
  if len(result1) != len(result2):
    printErrorMessageAndTerminate("different length")
  
  sortList1 = []
  sortList2 = []
  valueNum = 0
  for i in range(0, len(result1)):
    if i == 0 or (result1[i - 1] == result2[i - 1] and '-' in result1[i - 1]):
      header1 = result1[i]
      header2 = result2[i]
      if not isHeaderEqual(header1, header2):
        printErrorMessageAndTerminate(formatErrorMessage(0, "query header not the same"))
    elif result1[i] == result2[i] and '-' in result1[i]:
      sortList1.sort()
      sortList2.sort()
      if sortList1 != sortList2:
        printErrorMessageAndTerminate(formatErrorMessage(valueNum, "values not the same"))
      sortList1 = []
      sortList2 = []
      valueNum = valueNum + 1
    else:
      sortList1.append(result1[i])
      sortList2.append(result2[i])
  print("Results Matched Successfully")

def isHeaderEqual(header1, header2):
  header1 = header1.split(" ")
  header2 = header2.split(" ")

  if len(header1) != len(header2): 
    return False

  for i in range(0, len(header1)):
    if not isAggregationEqual(header1[i].lower(), header2[i].lower()):
      print(header1[i])
      print(header2[i])
      return False
  
  return True

def isAggregationEqual(value1, value2):
  if (value1 == value2):
    return True
  if "countof" in value1 and "count" in value2:
    return value1[7:] == extractValueBetweenBraces(value2)
  elif "avgof" in value1 and "avg" in value2:
    return value1[5:] == extractValueBetweenBraces(value2)
  elif "minof" in value1 and "min" in value2:
    return value1[5:] == extractValueBetweenBraces(value2)
  elif "maxof" in value1 and "max" in value2:
    return value1[5:] == extractValueBetweenBraces(value2)
  elif "sumof" in value1 and "sum" in value2:
    return value1[5:] == extractValueBetweenBraces(value2)
  return False


def extractValueBetweenBraces(header):
  result = re.search('\((.*)\)', header)
  return result.group(1)

def formatErrorMessage(num, msg):
  return "[query " + str(num + 1) + "] " + msg

def printErrorMessageAndTerminate(msg):
  print("Error: " + msg)
  sys.exit()


if __name__ == "__main__":
    main()