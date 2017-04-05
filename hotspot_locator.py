import math
import pandas as pd
import datetime as dt
import multiprocessing as mp
import os

LATITUDE_FEET  = 2.7500413975823255e-06  # Latitude per foot - based on center of Ohio
LONGITUDE_FEET = 3.6435606060606596e-06  # Longitude per foot - based on center of Ohio

SEARCH_RADIUS = 500  # feet
CITIES        = ["Cincinnati", "Dayton", "Akron", "Columbus", "Cleveland"]
OUTPUT_DIR    = 'output\\{}ft Search Radius -- {}'.format(str(SEARCH_RADIUS),
                                                          dt.datetime.now().strftime('%Y-%m-%d %H-%M-%S'))
INPUT_DIR     = 'data'

# cityCoords = [
#               (39.102674, -84.512615),  # Cincinnati
#               (39.762510, -84.192638),  # Dayton
#               (41.081360, -81.519475),  # Akron
#               (39.961509, -82.998907),  # Columbus
#               (41.500097, -81.692889),  # Cleveland
#               ]


def get_euclids(args):
    city, points, taskPoints = args

    # Iterate through points
    neighbors = pd.DataFrame(columns=['cnt', 'distFeet'], index=taskPoints.index)
    i = 0
    for k, p in taskPoints.iterrows():
        i += 1

        def euclid(row):
            latiDist = (p.Latitude - row.Latitude) / LATITUDE_FEET
            longDist = (p.Longitude - row.Longitude) / LONGITUDE_FEET
            return math.sqrt(latiDist ** 2 + longDist ** 2)

        if i % 50 == 0:
            ts = dt.datetime.now()
            dist = points.apply(euclid, axis=1)
            td = dt.datetime.now() - ts
            timeLeft = td * (len(taskPoints) - i)
            print("{} -- {}/{} -- {}".format(city, i, len(taskPoints), timeLeft))
        else:
            dist = points.apply(euclid, axis=1)
        msk = ((dist < SEARCH_RADIUS) & (dist > 0))
        matches = set(dist.index[msk])
        neighbors.loc[(p.Latitude, p.Longitude), ['cnt', 'distFeet']] = (len(matches), matches)

    return neighbors.loc[neighbors.cnt > 0]

if __name__ == '__main__':

    # Make output directory if it doesn't exist
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # Debug without multiprocessing
    # get_euclids(("Cincinnati", pd.DataFrame.from_csv("2016 Recoveries {}.csv".format("Cincinnati")).head(150)))

    # Break each city into groups and process simultaneously
    ts2 = dt.datetime.now()
    for cityName in CITIES:
        ts1 = dt.datetime.now()
        hotSpots = None
        processCnt = 6
        inFileName = "{}\\{}.csv".format(INPUT_DIR, cityName)
        try:
            repoPoints = pd.DataFrame.from_csv(inFileName)[['Longitude', 'Latitude']].head(100)
        except Exception as e:
            print("Failed to load {}, ".format(inFileName, e))
            break

        repoPoints.index = [(v.Latitude, v.Longitude) for k, v in repoPoints.iterrows()]
        taskGroups = pd.cut(range(len(repoPoints)), processCnt, labels=range(processCnt))

        with mp.Pool(processCnt) as pool:
            resList = pool.map(get_euclids,
                               ((cityName, repoPoints, repoPoints.loc[taskGroups == b]) for b in taskGroups.categories))

        hotSpots = pd.concat(resList)
        hotSpots.to_csv('{}\\{}.csv'.format(OUTPUT_DIR, cityName))
        print("{} -- Time Elapsed={}".format(cityName, dt.datetime.now() - ts1))

    print("TOTAL TIME ELAPSED={}".format(dt.datetime.now() - ts2))
