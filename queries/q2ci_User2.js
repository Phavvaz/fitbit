// Person2 Morning Steps
db.person2Steps.aggregate([
  {
    $addFields: {
      date: { $toDate: '$dateTime' },
      month: { $month: { $toDate: '$dateTime' } },
      time: {
        $arrayElemAt: [{ $split: [{ $trim: { input: '$dateTime' } }, ' '] }, 1],
      },
      step: '$value',
    },
  },
  {
    $match: {
      time: { $gte: '07:00:00', $lte: '12:00:00' },
    },
  },
  {
    $project: {
      date: 1,
      month: 1,
      time: 1,
      step: 1,
    },
  },
  {
    $merge: {
      into: { db: 'fitbitAggPerson2', coll: 'person2MorningSteps' },
    },
  },
]);

// Person2 Day Steps
db.person2Steps.aggregate([
  {
    $addFields: {
      date: { $toDate: '$dateTime' },
      month: { $month: { $toDate: '$dateTime' } },
      time: {
        $arrayElemAt: [{ $split: [{ $trim: { input: '$dateTime' } }, ' '] }, 1],
      },
      step: '$value',
    },
  },
  {
    $match: {
      $or: [{ time: { $gte: '12:01:00', $lte: '19:00:00' } }],
    },
  },
  {
    $project: {
      date: 1,
      month: 1,
      time: 1,
      step: 1,
    },
  },
  {
    $merge: { into: { db: 'fitbitAggPerson2', coll: 'person2DaySteps' } },
  },
]);

// Person2 Night Steps
db.person2Steps.aggregate([
  {
    $addFields: {
      date: { $toDate: '$dateTime' },
      month: { $month: { $toDate: '$dateTime' } },
      time: {
        $arrayElemAt: [{ $split: [{ $trim: { input: '$dateTime' } }, ' '] }, 1],
      },
      step: '$value',
    },
  },
  {
    $match: {
      $or: [
        { time: { $gte: '19:01:00', $lte: '23:59:00' } },
        { time: { $gte: '00:00:00', $lte: '06:59:00' } },
      ],
    },
  },
  {
    $project: {
      date: 1,
      month: 1,
      time: 1,
      step: 1,
    },
  },
  {
    $merge: { into: { db: 'fitbitAggPerson2', coll: 'person2NightSteps' } },
  },
]);

// Person2 Morning Calories
db.person2Calories.aggregate([
  {
    $addFields: {
      date: { $toDate: '$dateTime' },
      month: { $month: { $toDate: '$dateTime' } },
      time: {
        $arrayElemAt: [{ $split: [{ $trim: { input: '$dateTime' } }, ' '] }, 1],
      },
      calory: '$value',
    },
  },
  {
    $match: {
      time: { $gte: '07:00:00', $lte: '12:00:00' },
    },
  },
  {
    $project: {
      date: 1,
      month: 1,
      time: 1,
      calory: 1,
    },
  },
  {
    $merge: {
      into: { db: 'fitbitAggPerson2', coll: 'person2MorningCalories' },
    },
  },
]);

// Person2 Day Calories
db.person2Calories.aggregate([
  {
    $addFields: {
      date: { $toDate: '$dateTime' },
      month: { $month: { $toDate: '$dateTime' } },
      time: {
        $arrayElemAt: [{ $split: [{ $trim: { input: '$dateTime' } }, ' '] }, 1],
      },
      calory: '$value',
    },
  },
  {
    $match: {
      $or: [{ time: { $gte: '12:01:00', $lte: '19:00:00' } }],
    },
  },
  {
    $project: {
      date: 1,
      month: 1,
      time: 1,
      calory: 1,
    },
  },
  {
    $merge: { into: { db: 'fitbitAggPerson2', coll: 'person2DayCalories' } },
  },
]);

// Person2 Night Calories
db.person2Calories.aggregate([
  {
    $addFields: {
      date: { $toDate: '$dateTime' },
      month: { $month: { $toDate: '$dateTime' } },
      time: {
        $arrayElemAt: [{ $split: [{ $trim: { input: '$dateTime' } }, ' '] }, 1],
      },
      calory: '$value',
    },
  },
  {
    $match: {
      $or: [
        { time: { $gte: '19:01:00', $lte: '23:59:00' } },
        { time: { $gte: '00:00:00', $lte: '06:59:00' } },
      ],
    },
  },
  {
    $project: {
      date: 1,
      month: 1,
      time: 1,
      calory: 1,
    },
  },
  {
    $merge: { into: { db: 'fitbitAggPerson2', coll: 'person2NightCalories' } },
  },
]);

// Avg Morning Steps for Person 2
db.person2MorningSteps.aggregate([
  {
    $addFields: {
      convertedSteps: { $toDouble: '$step' },
    },
  },
  {
    $group: {
      _id: null,
      User2AvgMorningSteps: { $avg: { $sum: '$convertedSteps' } },
    },
  },
  {
    $project: {
      _id: 0,
      User2AvgMorningSteps: 1,
    },
  },
  {
    $merge: {
      into: { db: 'fitbitPerson2AvgSteps', coll: 'person2AvgMorningSteps' },
    },
  },
]);

// Avg Day Steps for Person 2
db.person2DaySteps.aggregate([
  {
    $addFields: {
      convertedSteps: { $toDouble: '$step' },
    },
  },
  {
    $group: {
      _id: null,
      User2AvgDaySteps: { $avg: { $sum: '$convertedSteps' } },
    },
  },
  {
    $project: {
      _id: 0,
      User2AvgDaySteps: 1,
    },
  },
  {
    $merge: {
      into: { db: 'fitbitPerson2AvgSteps', coll: 'person2AvgDaySteps' },
    },
  },
]);

// Avg Night Steps for Person 2
db.person2NightSteps.aggregate([
  {
    $addFields: {
      convertedSteps: { $toDouble: '$step' },
    },
  },
  {
    $group: {
      _id: null,
      User2AvgNightSteps: { $avg: { $sum: '$convertedSteps' } },
    },
  },
  {
    $project: {
      _id: 0,
      User2AvgNightSteps: 1,
    },
  },
  {
    $merge: {
      into: { db: 'fitbitPerson2AvgSteps', coll: 'person2AvgNightSteps' },
    },
  },
]);

// Avg Morning Calories for Person 2
db.person2MorningCalories.aggregate([
  {
    $addFields: {
      convertedCalories: { $toDouble: '$calory' },
    },
  },
  {
    $group: {
      _id: null,
      User2AvgMorningCalories: { $avg: { $sum: '$convertedCalories' } },
    },
  },
  {
    $project: {
      _id: 0,
      User2AvgMorningCalories: 1,
    },
  },
  {
    $merge: {
      into: {
        db: 'fitbitPerson2AvgCalories',
        coll: 'person2AvgMorningCalories',
      },
    },
  },
]);

// Avg Day Calories for Person 2
db.person2DayCalories.aggregate([
  {
    $addFields: {
      convertedCalories: { $toDouble: '$calory' },
    },
  },
  {
    $group: {
      _id: null,
      User2AvgDayCalories: { $avg: { $sum: '$convertedCalories' } },
    },
  },
  {
    $project: {
      _id: 0,
      User2AvgDayCalories: 1,
    },
  },
  {
    $merge: {
      into: {
        db: 'fitbitPerson2AvgCalories',
        coll: 'person2AvgDayCalories',
      },
    },
  },
]);

// Avg Night Calories for Person 2
db.person2NightCalories.aggregate([
  {
    $addFields: {
      convertedCalories: { $toDouble: '$calory' },
    },
  },
  {
    $group: {
      _id: null,
      User2AvgNightCalories: { $avg: { $sum: '$convertedCalories' } },
    },
  },
  {
    $project: {
      _id: 0,
      User2AvgNightCalories: 1,
    },
  },
  {
    $merge: {
      into: {
        db: 'fitbitPerson2AvgCalories',
        coll: 'person2AvgNightCalories',
      },
    },
  },
]);

// Avg Steps in the morning for Person2 in August
db.person2MorningSteps.aggregate([
  {
    $addFields: {
      convertedSteps: { $toDouble: '$step' },
    },
  },
  {
    $match: { month: 8 },
  },
  {
    $group: {
      _id: null,
      User2AvgAugMorningSteps: { $avg: { $sum: '$convertedSteps' } },
    },
  },
  {
    $project: {
      _id: 0,
      User2AvgAugMorningSteps: 1,
    },
  },
]);
