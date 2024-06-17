# Todo : la plupart des methodes dans oeilnc_util.geometry devrait être ici logiquement.

import geopandas as gpd
from shapely.geometry import Polygon, Point
import numpy as np
from sklearn.cluster import KMeans
from scipy.spatial import Voronoi
from shapely.ops import voronoi_diagram, unary_union

def generate_random_points(polygon, num_points):
    """
    Generates random points within a given polygon.

    Args:
        polygon (Polygon): The polygon within which the random points will be generated.
        num_points (int): The number of random points to generate.

    Returns:
        list: A list of Point objects representing the generated random points.
    """
    min_x, min_y, max_x, max_y = polygon.bounds
    points = []
    while len(points) < num_points:
        random_point = Point(np.random.uniform(min_x, max_x), np.random.uniform(min_y, max_y))
        if polygon.contains(random_point):
            points.append(random_point)
    return points

def voronoiSplitting(polygon, nbPoint, num_clusters, crs):
    """
    Generate Voronoi polygons by splitting a given polygon into clusters of random points.

    Args:
        polygon (shapely.geometry.Polygon): The input polygon to be split.
        nbPoint (int): The number of random points to generate.

    Returns:
        geopandas.GeoDataFrame: A GeoDataFrame containing the intersected polygons of the Voronoi diagram.

    """

    points = generate_random_points(polygon, nbPoint)

    # Convertir les points en GeoDataFrame
    points_gdf = gpd.GeoDataFrame(geometry=points, crs=crs)

    # 2. Utiliser KMeans pour clusteriser les points
    kmeans = KMeans(n_clusters=num_clusters)
    points_gdf['cluster'] = kmeans.fit_predict([(point.x, point.y) for point in points_gdf.geometry])

    # 3. Calculer les centroïdes des clusters
    centroids = points_gdf.groupby('cluster')['geometry'].apply(lambda x: x.unary_union.centroid)

    # 4. Générer les polygones de Voronoi
    vor = Voronoi(np.array([(centroid.x, centroid.y) for centroid in centroids]))
    voronoi_shapes = [Polygon(vor.vertices[line]) for line in vor.regions if len(line) > 0 and -1 not in line]

    # Convertir les polygones de Voronoi en GeoDataFrame
    voronoi_gdf = gpd.GeoDataFrame(geometry=voronoi_shapes, crs=crs)

    # 5. Intersecter les polygones de Voronoi avec le polygone original
    polygon_gdf = gpd.GeoDataFrame(geometry=[polygon], crs=crs)
    intersection = gpd.overlay(polygon_gdf, voronoi_gdf, how='intersection')

    return intersection

    