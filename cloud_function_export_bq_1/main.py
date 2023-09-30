import functions_framework
import ee
import time
import google.auth

@functions_framework.http
def write_to_bq(request):
  # Set up auth.
  credentials, project_id = google.auth.default()
  ee.Initialize(credentials, project="okalashnikava-ee")

  # The area of interest ("AOI") polygon, can also be imported or hand-drawn.
  aoi = ee.Geometry.Polygon(
    [[[18.51964108409259, 54.460327809570266],
     [18.51964108409259, 54.29555815329559],
      [18.76408688487384, 54.29555815329559],
       [18.76408688487384, 54.460327809570266]]], None, False)

  # Load the Sentinel-1 collection (log scaling, VV co-polar).
  collection = ee.ImageCollection('COPERNICUS/S1_GRD').filterBounds(aoi).filter(ee.Filter.eq('instrumentMode', 'IW')).filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV')).filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VH')).filter(ee.Filter.eq('orbitProperties_pass', 'DESCENDING')).select(['VV', 'VH']).filterDate("2023-09-15", "2023-09-21").median();

  # Remove global surface water (oceans, lakes, etc.).
  jrcData0 = ee.Image('JRC/GSW1_0/Metadata').select('total_obs').lte(0)
  # mask for persistent water (more than 50% of the time)
  waterMask = ee.Image('JRC/GSW1_0/GlobalSurfaceWater').select('occurrence').unmask(0).max(jrcData0).lt(50);  

  floodedPixels = collection.updateMask(waterMask)

  # Convert the patches of pixels to polygons (vectors).
  vectors = floodedPixels.sample(region=aoi, scale=100, geometries=True)

  vectors = vectors.map(lambda f: f.set('date', '2023-09-15'))

  task = ee.batch.Export.table.toBigQuery(collection=vectors, 
    description='bq_warsaw_looker_demo',
    table='okalashnikava-ee.ee_export.bq_warsaw_looker_demo',
    append=True
  )

  task.start()
