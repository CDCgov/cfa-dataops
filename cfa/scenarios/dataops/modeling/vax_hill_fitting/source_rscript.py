from argparse import ArgumentParser, Namespace
import sys
import subprocess as sp

def run(path):
    #run r script from here
    try:
        # Run the R script using subprocess
        result = sp.run(
            ["Rscript", path],
            check=True,
            capture_output=True,
            text=True,
            shell=True
        )
        print("R script output:", result.stdout)
    except sp.CalledProcessError as e:
        print("Error running R script:", e.stderr)
        sys.exit(1)

if __name__ == "__main__":
    #setup argparsers
    parser = ArgumentParser(description="Run the specified r script.")
    parser.add_argument("-p", "--path", type=str, default = "all",required=False, help="path to the R script to run")
    args = parser.parse_args()


    # Convert args to Namespace
    args = Namespace(
        path=args.path
    )
    # Run the main function
    run(path = args.path)
