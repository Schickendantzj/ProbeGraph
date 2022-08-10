import pandas as pd
import matplotlib.pyplot as plt

__FILENAMES__ = {
    "foreign": "foreign-",
    "self": "self-",
    "download": "throughput-download",
    "upload": "throughput-upload"
}


def renameFiles(filename):
    global foreign_path
    global self_path
    global download_path
    global upload_path
    foreign_path = filename.format(__FILENAMES__["foreign"])
    self_path = filename.format(__FILENAMES__["self"])
    download_path = filename.format(__FILENAMES__["download"])
    upload_path =filename.format(__FILENAMES__["upload"])


def filenameGenerator(start, end):
    filenameFormat = start + "{}" + end
    return filenameFormat



def findEarliest(dfs):
    """
    Assumes sorted dfs
    :param dfs:
    :return:
    """
    earliest = dfs[0]["CreationTime"][0]
    for df in dfs:
        if earliest > df["CreationTime"][0]:
            earliest = df["CreationTime"][0]
    return earliest


def timeSinceStart(dfs, start):
    """
    Adds "TimeSinceStart" column to all dataframes
    :param dfs:
    :param start:
    :return:
    """
    for df in dfs:
        df["TimeSinceStart"] = df["CreationTime"]-start


def secondsSinceStart(dfs, start):
    """
    Adds "SecondsSinceStart" column to all dataframes
    :param dfs:
    :param start:
    :return:
    """
    for df in dfs:
        df["SecondsSinceStart"] = (df["CreationTime"]-start).apply(pd.Timedelta.total_seconds)


def timeToInt(x):
    if "ms" in x:
        return float(x[:-2])
    if "s" in x:
        return float(x[:-1]) * 1000


def probeClean(df):
    df.columns = ["CreationTime", "NumRTT", "Duration", "Empty"]
    df = df.drop(columns=["Empty"])
    df["CreationTime"] = pd.to_datetime(df["CreationTime"], format="%m-%d-%Y-%H-%M-%S.%f")
    # df["TimeSinceStart"] = df["CreationTime"]-df["CreationTime"][0]
    # df["SecondsSinceStart"] = df["TimeSinceStart"].apply(pd.Timedelta.total_seconds)
    df["Duration"] = df["Duration"].apply(timeToInt)
    df["ADJ_Duration"] = df["Duration"] / df["NumRTT"]
    df = df.sort_values(by=["CreationTime"])
    return df


def throughputClean(df):
    df.columns = ["CreationTime", "Throughput", "Empty"]
    df = df.drop(columns=["Empty"])
    df["CreationTime"] = pd.to_datetime(df["CreationTime"], format="%m-%d-%Y-%H-%M-%S.%f")
    # df["TimeSinceStart"] = df["CreationTime"] - df["CreationTime"][0]
    # df["SecondsSinceStart"] = df["TimeSinceStart"].apply(pd.Timedelta.total_seconds)
    df["ADJ_Throughput"] = df["Throughput"] / 1000000
    df = df.sort_values(by=["CreationTime"])
    return df


def main(title):
    # Data Ingestion
    foreign = pd.read_csv(foreign_path)
    self = pd.read_csv(self_path)
    download = pd.read_csv(download_path)
    upload = pd.read_csv(upload_path)

    # Data Cleaning
    foreign = probeClean(foreign)
    self = probeClean(self)
    download = throughputClean(download)
    upload = throughputClean(upload)

    # Moving Average
    foreign["DurationMA5"] = foreign["ADJ_Duration"].rolling(window=5).mean()
    self["DurationMA5"] = self["ADJ_Duration"].rolling(window=5).mean()

    # Normalize
    dfs = [foreign, self, download, upload]
    timeSinceStart(dfs, findEarliest(dfs))
    secondsSinceStart(dfs, findEarliest(dfs))

    print(foreign.info())

    yCol = "SecondsSinceStart"

    # Graphing
    fig, ax = plt.subplots()
    ax.set_title(title)
    ax.plot(foreign[yCol], foreign["ADJ_Duration"], "b.", label="foreign")
    ax.plot(self[yCol], self["ADJ_Duration"], "r.", label="self")
    ax.plot(foreign[yCol], foreign["DurationMA5"], "b--", label="foreignMA")
    ax.plot(self[yCol], self["DurationMA5"], "r--", label="selfMA")
    ax.set_ylim([-10, max(foreign["ADJ_Duration"].max(), self["ADJ_Duration"].max())])
    ax.legend(loc="upper left")

    secax = ax.twinx()
    secax.plot(download[yCol], download["ADJ_Throughput"], "g-", label="download (MB/s)")
    secax.plot(upload[yCol], upload["ADJ_Throughput"], "y-", label="upload (MB/s)")
    secax.legend(loc="upper right")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # Initialize
    foreign_path = ""
    self_path = ""
    download_path = ""
    upload_path = ""


    # renameFiles(filenameGenerator("./Data/APPL-", "08-04-2022-12-53-05.csv"))
    # main("APPLE")

    foreign_path = "./Data/APPL-foreign-08-04-2022-12-53-05.csv"
    self_path = "./Data/APPL-self-08-04-2022-12-53-05.csv"
    download_path = "./Data/APPL-throughput-download08-04-2022-12-53-05.csv"
    upload_path = "./Data/APPL-throughput-upload08-04-2022-12-53-05.csv"
    main("APPLE")
    foreign_path = "./Data/NGINX-foreign-08-04-2022-15-04-39.csv"
    self_path = "./Data/NGINX-self-08-04-2022-15-04-39.csv"
    download_path = "./Data/NGINX-throughput-download08-04-2022-15-04-39.csv"
    upload_path = "./Data/NGINX-throughput-upload08-04-2022-15-04-39.csv"
    main("NGINX")
    foreign_path = "./Data/GO-foreign-08-04-2022-15-13-44.csv"
    self_path = "./Data/GO-self-08-04-2022-15-13-44.csv"
    download_path = "./Data/GO-throughput-download08-04-2022-15-13-44.csv"
    upload_path = "./Data/GO-throughput-upload08-04-2022-15-13-44.csv"
    main("GOLANG")
    foreign_path = "./Data/TRY-foreign-08-04-2022-18-40-48.csv"
    self_path = "./Data/TRY-self-08-04-2022-18-40-48.csv"
    download_path = "./Data/TRY-throughput-download08-04-2022-18-40-48.csv"
    upload_path = "./Data/TRY-throughput-upload08-04-2022-18-40-48.csv"
    main("TRY")
    foreign_path = "./Data/TRY2-foreign-08-05-2022-00-31-54.csv"
    self_path = "./Data/TRY2-self-08-05-2022-00-31-54.csv"
    download_path = "./Data/TRY2-throughput-download08-05-2022-00-31-54.csv"
    upload_path = "./Data/TRY2-throughput-upload08-05-2022-00-31-54.csv"
    main("TRY2")
    renameFiles(filenameGenerator("./Data/NGINX27-", "08-05-2022-15-03-43.csv"))
    main("NGINX27")
    plt.show()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
