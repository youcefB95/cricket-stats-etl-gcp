let isHeader = true;

function transform(line) {
  if (isHeader) {
    isHeader = false; // Passer l'en-tête
    return null; // Ne rien faire pour la première ligne
  }

  var values = line.split(",");
  var obj = new Object();
  obj.id = values[0];
  obj.rank = values[1];
  obj.name = values[2];
  obj.country = values[3];
  obj.rating = values[4];
  obj.points = values[5];
  obj.lastUpdatedOn = values[6];
  obj.trend = values[7];
  obj.faceImageId = values[8];
  obj.countryId = values[9];
  obj.difference = values[10];

  var jsonString = JSON.stringify(obj);
  return jsonString;
}
