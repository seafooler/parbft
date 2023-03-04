from logs import LogParser, ParseError
def main():
    try:
        print(LogParser.process('./logs').result())
    except ParseError as e:
        print('Failed to parse logs', e)
if __name__ == "__main__":
    main()