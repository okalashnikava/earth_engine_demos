import functions_framework
import ee
import time
import google.auth

@functions_framework.http
def write_to_bq(request):

  # Set up auth.
  credentials, project_id = google.auth.default()
  ee.Initialize(credentials, project=project_id)
  
  # Fetch the current time.
  epoch = int(time.time_ns() / 1_000_000)
  now = ee.Date(epoch)

  # Define last two weeks
  currentWeek = now.advance(-1, 'week').getRange('week')
  previousWeek = currentWeek.start().advance(-1, 'week').getRange('week')
  
  currentStartDate = currentWeek.start().format('YYYY-MM-dd').getInfo()
  currentEndDate = currentWeek.end().format('YYYY-MM-dd').getInfo()

  previousStartDate = previousWeek.start().format('YYYY-MM-dd').getInfo()
  previousEndDate = previousWeek.end().format('YYYY-MM-dd').getInfo()

  # The area of interest ("AOI") polygon, can also be imported or hand-drawn.
  aoi = ee.Geometry.Polygon(
    [[[18.51964108409259, 54.460327809570266],
     [18.51964108409259, 54.29555815329559],
      [18.76408688487384, 54.29555815329559],
       [18.76408688487384, 54.460327809570266]]], None, False)
  
  # Smooth the data to remove noise.
  SMOOTHING_RADIUS_METERS = 100

  # Load the Sentinel-1 collection (log scaling, VV co-polar).
  collection = ee.ImageCollection('COPERNICUS/S1_GRD').filterBounds(aoi).filter(ee.Filter.eq('instrumentMode', 'IW')).filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV')).filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VH')).filter(ee.Filter.eq('orbitProperties_pass', 'DESCENDING')).select('VV')
  after = collection.filterDate(currentStartDate, currentEndDate).mosaic().focalMedian(SMOOTHING_RADIUS_METERS, 'circle', 'meters')
  before = collection.filterDate(previousStartDate, previousEndDate).mosaic().focalMedian(SMOOTHING_RADIUS_METERS, 'circle', 'meters')

  # Threshold smoothed radar intensities to identify areas with standing water.
  DIFF_THRESHOLD_DB = -3
  diffSmoothed = after.subtract(before)
  diffThresholded = diffSmoothed.lt(DIFF_THRESHOLD_DB)

  # Remove global surface water (oceans, lakes, etc.).
  jrcData0 = ee.Image('JRC/GSW1_0/Metadata').select('total_obs').lte(0)
  # mask for persistent water (more than 50% of the time)
  waterMask = ee.Image('JRC/GSW1_0/GlobalSurfaceWater').select('occurrence').unmask(0).max(jrcData0).lt(50);  

  changedPixels = diffThresholded.updateMask(waterMask)

  # Convert the patches of pixels to polygons (vectors).
  vectors = changedPixels.reduceToVectors(
    geometry=aoi,
    scale=10,
    geometryType='polygon',
    eightConnected= False  # only connect if pixels share an edge
  )
  
  # Eliminate large features in the dataset.
  MAX_AREA = 500 * 1000  # m^2
  vectors = vectors.map(lambda f: f.set('date', currentStartDate).set('area', f.geometry().area(10))).filter(ee.Filter.lt("area", MAX_AREA))

  task = ee.batch.Export.table.toBigQuery(collection=vectors, 
    description='bq_warsaw_looker_demo',
    table='okalashnikava-experimental.ee_export.bq_warsaw_looker_demo',
    append=True
  )

  task.start()
  return f"Started task {task.id}."
