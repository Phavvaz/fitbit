// Person1 Steps in July
db.person1Steps.aggregate([
  {
    $addFields: {
      monthJuly: { $month: { $toDate: '$dateTime' } },
    },
  },
  {
    $match: { monthJuly: 7 },
  },
  {
    $group: {
      _id: null,
      stepsCount: { $sum: 1 },
    },
  },
  {
    $project: {
      _id: 0,
      month: 'July',
      stepsCount: 1,
    },
  },
]);

// Person2 Steps in July
db.person2Steps.aggregate([
  {
    $addFields: {
      monthJuly: { $month: { $toDate: '$dateTime' } },
    },
  },
  {
    $match: { monthJuly: 7 },
  },
  {
    $group: {
      _id: null,
      stepsCount: { $sum: 1 },
    },
  },
  {
    $project: {
      _id: 0,
      month: 'July',
      stepsCount: 1,
    },
  },
]);
