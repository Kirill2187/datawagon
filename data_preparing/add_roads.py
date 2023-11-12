import pandas as pd


def add_roads(src: pd.DataFrame, path_train: str):
    if 'road_id' in src.columns:
        print('road_id already in df. Exitting')
        return src

    stations_data = pd.read_parquet(path_train + '/stations.parquet').convert_dtypes()

    functions = {}

    cols = ['st_border_sign', 'st_sea_sign', 'st_river_sign',
            'st_car_sign', 'st_ferry_sign', 'st_freigh_sign', 'opor_station_sign']

    for col in cols:
        stations_data[col] = pd.to_numeric(stations_data[col], errors='coerce')
        functions[col] = ['sum']
    roads = pd.DataFrame(stations_data.groupby('road_id').agg(functions))
    roads = roads.droplevel(1, axis=1)

    station_to_road = stations_data[['st_id', 'road_id']].drop_duplicates()

    data = src.merge(station_to_road, how='left', left_on='st_id_dest', right_on='st_id')
    data = data.merge(station_to_road, how='left', left_on='st_id_send', right_on='st_id')

    data['road_id_x'].fillna(data['road_id_y'], inplace=True)
    data.rename(columns={'road_id_x': 'road_id'}, inplace=True)
    data.drop(columns=['road_id_y'], inplace=True)

    data = data.merge(roads, how='left', on='road_id')

    return data


if __name__ == '__main__':
    PATH = '???'
    df = pd.read_csv(PATH)
    df = add_roads(df)
    df.to_csv(PATH)
