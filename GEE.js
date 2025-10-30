// ===================================================================
// SKRIP GABUNGAN: MENGHITUNG NTL DAN NO2 PER PROVINSI (2019-2024)
// ===================================================================

// ===================================================================
// BAGIAN 1: PENGATURAN AWAL (SETUP)
// ===================================================================

// Pastikan path ini sesuai dengan aset shapefile provinsi Anda
// Menggunakan aset yang Anda berikan.
var provinsi = ee.FeatureCollection('projects/login-414922/assets/prov');

// Daftar tahun yang ingin dianalisis
var years = [2019, 2020, 2021, 2022, 2023, 2024];

// Pusatkan peta ke Indonesia dan tampilkan batas provinsi
Map.centerObject(provinsi, 5);
Map.addLayer(provinsi, {color: 'FF0000'}, 'Batas Provinsi'); // Merah agar jelas


// ===================================================================
// BAGIAN 2: FUNGSI-FUNGSI PERHITUNGAN
// Kita buat dua fungsi terpisah: satu untuk NTL, satu untuk NO2.
// ===================================================================

/**
 * Fungsi untuk menghitung total NTL (Night Time Lights) untuk tahun tertentu.
 * @param {number} year - Tahun yang akan dianalisis.
 * @returns {ee.FeatureCollection} - Fitur provinsi dengan kolom NTL baru.
 */
var calculateNtlForYear = function(year) {
  var startDate = ee.Date.fromYMD(year, 1, 1);
  var endDate = ee.Date.fromYMD(year, 12, 31);

  // Filter koleksi VIIRS NTL, pilih band 'avg_rad', dan hitung rata-rata tahunan
  var ntlImage = ee.ImageCollection('NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG')
                      .filterDate(startDate, endDate)
                      .select('avg_rad')
                      .mean();

  // Hitung jumlah total cahaya (sum of lights) untuk setiap provinsi
  var ntlSum = ntlImage.reduceRegions({
    collection: provinsi,
    reducer: ee.Reducer.sum(),
    scale: 500
  });

  // Ubah nama kolom 'sum' menjadi nama yang unik (misal: 'NTL_2019')
  // Ganti 'NAME_1' jika nama kolom provinsi Anda berbeda
  return ntlSum.map(function(feature) {
    return feature.select(['NAME_1']).set('NTL_' + year, feature.get('sum'));
  });
};


/**
 * Fungsi untuk menghitung total NO2 (Nitrogen Dioksida) untuk tahun tertentu.
 * @param {number} year - Tahun yang akan dianalisis.
 * @returns {ee.FeatureCollection} - Fitur provinsi dengan kolom NO2 baru.
 */
var calculateNo2ForYear = function(year) {
  var startDate = ee.Date.fromYMD(year, 1, 1);
  var endDate = ee.Date.fromYMD(year, 12, 31);
  
  // Filter koleksi Sentinel-5P NO2 dan hitung rata-rata tahunan
  var no2Image = ee.ImageCollection('COPERNICUS/S5P/OFFL/L3_NO2')
                     .filterDate(startDate, endDate)
                     .select('tropospheric_NO2_column_number_density')
                     .mean();
                     
  // Hitung jumlah total NO2 untuk setiap provinsi
  var no2Sum = no2Image.reduceRegions({
    collection: provinsi,
    reducer: ee.Reducer.sum(),
    scale: 5000 // Skala disesuaikan dengan resolusi Sentinel-5P
  });

  // Ubah nama kolom 'sum' menjadi nama yang unik (misal: 'NO2_2019')
  // Ganti 'NAME_1' jika nama kolom provinsi Anda berbeda
  return no2Sum.map(function(feature) {
    return feature.select(['NAME_1']).set('NO2_' + year, feature.get('sum'));
  });
};


// ===================================================================
// BAGIAN 3: PROSES SEMUA TAHUN DAN GABUNGKAN HASILNYA
// ===================================================================

// Mulai dengan FeatureCollection yang hanya berisi nama provinsi sebagai dasar
var combinedResults = provinsi.select(['NAME_1']);

// Siapkan filter dan join yang akan digunakan berulang kali di dalam loop
var filter = ee.Filter.equals({ leftField: 'NAME_1', rightField: 'NAME_1' });
var join = ee.Join.inner();

// Lakukan perulangan (loop) untuk setiap tahun dalam daftar 'years'
years.forEach(function(year) {
  print('Memproses tahun: ' + year);
  
  // 1. Hitung NTL untuk tahun saat ini
  var ntlYearlyResult = calculateNtlForYear(year);
  
  // 2. Hitung NO2 untuk tahun saat ini
  var no2YearlyResult = calculateNo2ForYear(year);
  
  // 3. Gabungkan hasil NTL ke tabel utama
  var joinedNtl = join.apply(combinedResults, ntlYearlyResult, filter);
  combinedResults = joinedNtl.map(function(feature) {
    return ee.Feature(feature.get('primary')).copyProperties(feature.get('secondary'));
  });
  
  // 4. Gabungkan hasil NO2 ke tabel utama (yang sekarang sudah berisi NTL)
  var joinedNo2 = join.apply(combinedResults, no2YearlyResult, filter);
  combinedResults = joinedNo2.map(function(feature) {
    return ee.Feature(feature.get('primary')).copyProperties(feature.get('secondary'));
  });
});

// Cetak hasil akhir di Console untuk diperiksa sebelum ekspor
print('Hasil Gabungan NTL dan NO2 per Provinsi (2019-2024):', combinedResults);


// ===================================================================
// BAGIAN 4: EKSPOR HASIL AKHIR KE GOOGLE DRIVE
// ===================================================================

Export.table.toDrive({
  collection: combinedResults,
  description: 'NTL_dan_NO2_per_Provinsi_2019-2024', // Nama file CSV yang baru
  fileFormat: 'CSV'
});
