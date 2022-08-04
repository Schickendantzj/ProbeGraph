import pandas as pd
import matplotlib.pyplot as plt

foreign_path = "./Data/APPL-foreign-08-04-2022-12-53-05.csv"
self_path = "./Data/APPL-self-08-04-2022-12-53-05.csv"
download_path = "./Data/APPL-throughput-download08-04-2022-12-53-05.csv"
upload_path = "./Data/APPL-throughput-upload08-04-2022-12-53-05.csv"


def timeToInt(x):
    if "ms" in x:
        return float(x[:-2])
    if "s" in x:
        return float(x[:-1]) * 1000

def probeClean(df):
    df.columns = ["CreationTime", "NumRTT", "Duration", "Empty"]
    df = df.drop(columns=["Empty"])
    df["CreationTime"] = pd.to_datetime(df["CreationTime"], format="%m-%d-%Y-%H-%M-%S.%f")
    df["TimeSinceStart"] = df["CreationTime"]-df["CreationTime"][0]
    df["SecondsSinceStart"] = df["TimeSinceStart"].apply(pd.Timedelta.total_seconds)
    df["Duration"] = df["Duration"].apply(timeToInt)
    df["ADJ_Duration"] = df["Duration"] / df["NumRTT"]
    df = df.sort_values(by=["TimeSinceStart"])
    return df

def throughputClean(df):
    df.columns = ["CreationTime", "Throughput", "Empty"]
    df = df.drop(columns=["Empty"])
    df["CreationTime"] = pd.to_datetime(df["CreationTime"], format="%m-%d-%Y-%H-%M-%S.%f")
    df["TimeSinceStart"] = df["CreationTime"] - df["CreationTime"][0]
    df["SecondsSinceStart"] = df["TimeSinceStart"].apply(pd.Timedelta.total_seconds)
    df["ADJ_Throughput"] = df["Throughput"] / 1000000
    df = df.sort_values(by=["TimeSinceStart"])
    return df

def main():
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

    print(foreign.head(20))
    # Graphing
    fig, ax = plt.subplots()
    ax.plot(foreign["CreationTime"], foreign["ADJ_Duration"], "b.", label="foreign")
    ax.plot(self["CreationTime"], self["ADJ_Duration"], "r.", label="self")
    ax.plot(foreign["CreationTime"], foreign["DurationMA5"], "b--", label="foreignMA")
    ax.plot(self["CreationTime"], self["DurationMA5"], "r--", label="selfMA")
    ax.legend(loc="upper left")

    secax = ax.twinx()
    secax.plot(download["CreationTime"], download["ADJ_Throughput"], "g-", label="download (MB/s)")
    secax.plot(upload["CreationTime"], upload["ADJ_Throughput"], "y-", label="upload (MB/s)")
    secax.legend(loc="upper right")
    plt.show()

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
