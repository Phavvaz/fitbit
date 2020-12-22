//Avg Calories Burned by User1 everyday in July
db.person1Calories.aggregate([
  {
    $addFields: {
      getMonth: { $month: { $toDate: '$dateTime' } },
      convertedCalory: { $toDouble: '$value' },
    },
  },
  {
    $match: { getMonth: 7 },
  },
  {
    $group: {
      _id: null,
      avgCalories: { $avg: { $sum: '$convertedCalory' } },
    },
  },
  {
    $project: {
      _id: 0,
      month: 'July',
      avgCalories: 1,
    },
  },
]);

//Avg Calories Burned by User2 everyday in July
db.person2Calories.aggregate([
  {
    $addFields: {
      getMonth: { $month: { $toDate: '$dateTime' } },
      convertedCalory: { $toDouble: '$value' },
    },
  },
  {
    $match: { getMonth: 7 },
  },
  {
    $group: {
      _id: null,
      avgCalories: { $avg: { $sum: '$convertedCalory' } },
    },
  },
  {
    $project: {
      _id: 0,
      month: 'July',
      avgCalories: 1,
    },
  },
]);
