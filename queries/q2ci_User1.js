// Person1 Morning Steps
db.person1Steps.aggregate([
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
      into: { db: 'fitbitAggPerson1', coll: 'person1MorningSteps' },
    },
  },
]);

// Person1 Day Steps
db.person1Steps.aggregate([
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
    $merge: { into: { db: 'fitbitAggPerson1', coll: 'person1DaySteps' } },
  },
]);

// Person1 Night Steps
db.person1Steps.aggregate([
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
    $merge: { into: { db: 'fitbitAggPerson1', coll: 'person1NightSteps' } },
  },
]);

// Person1 Morning Calories
db.person1Calories.aggregate([
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
      into: { db: 'fitbitAggPerson1', coll: 'person1MorningCalories' },
    },
  },
]);

// Person1 Day Calories
db.person1Calories.aggregate([
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
    $merge: { into: { db: 'fitbitAggPerson1', coll: 'person1DayCalories' } },
  },
]);

// Person1 Night Calories
db.person1Calories.aggregate([
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
    $merge: { into: { db: 'fitbitAggPerson1', coll: 'person1NightCalories' } },
  },
]);

// Avg Morning Steps for Person 1
db.person1MorningSteps.aggregate([
  {
    $addFields: {
      convertedSteps: { $toDouble: '$step' },
    },
  },
  {
    $group: {
      _id: null,
      User1AvgMorningSteps: { $avg: { $sum: '$convertedSteps' } },
    },
  },
  {
    $project: {
      _id: 0,
      User1AvgMorningSteps: 1,
    },
  },
  {
    $merge: {
      into: { db: 'fitbitPerson1AvgSteps', coll: 'person1AvgMorningSteps' },
    },
  },
]);

// Avg Day Steps for Person 1
db.person1DaySteps.aggregate([
  {
    $addFields: {
      convertedSteps: { $toDouble: '$step' },
    },
  },
  {
    $group: {
      _id: null,
      User1AvgDaySteps: { $avg: { $sum: '$convertedSteps' } },
    },
  },
  {
    $project: {
      _id: 0,
      User1AvgDaySteps: 1,
    },
  },
  {
    $merge: {
      into: { db: 'fitbitPerson1AvgSteps', coll: 'person1AvgDaySteps' },
    },
  },
]);

// Avg Night Steps for Person 1
db.person1NightSteps.aggregate([
  {
    $addFields: {
      convertedSteps: { $toDouble: '$step' },
    },
  },
  {
    $group: {
      _id: null,
      User1AvgNightSteps: { $avg: { $sum: '$convertedSteps' } },
    },
  },
  {
    $project: {
      _id: 0,
      User1AvgNightSteps: 1,
    },
  },
  {
    $merge: {
      into: { db: 'fitbitPerson1AvgSteps', coll: 'person1AvgNightSteps' },
    },
  },
]);

// Avg Morning Calories for Person 1
db.person1MorningCalories.aggregate([
  {
    $addFields: {
      convertedCalories: { $toDouble: '$calory' },
    },
  },
  {
    $group: {
      _id: null,
      User1AvgMorningCalories: { $avg: { $sum: '$convertedCalories' } },
    },
  },
  {
    $project: {
      _id: 0,
      User1AvgMorningCalories: 1,
    },
  },
  {
    $merge: {
      into: {
        db: 'fitbitPerson1AvgCalories',
        coll: 'person1AvgMorningCalories',
      },
    },
  },
]);

// Avg Day Calories for Person 1
db.person1DayCalories.aggregate([
  {
    $addFields: {
      convertedCalories: { $toDouble: '$calory' },
    },
  },
  {
    $group: {
      _id: null,
      User1AvgDayCalories: { $avg: { $sum: '$convertedCalories' } },
    },
  },
  {
    $project: {
      _id: 0,
      User1AvgDayCalories: 1,
    },
  },
  {
    $merge: {
      into: {
        db: 'fitbitPerson1AvgCalories',
        coll: 'person1AvgDayCalories',
      },
    },
  },
]);

// Avg Night Calories for Person 1
db.person1NightCalories.aggregate([
  {
    $addFields: {
      convertedCalories: { $toDouble: '$calory' },
    },
  },
  {
    $group: {
      _id: null,
      User1AvgNightCalories: { $avg: { $sum: '$convertedCalories' } },
    },
  },
  {
    $project: {
      _id: 0,
      User1AvgNightCalories: 1,
    },
  },
  {
    $merge: {
      into: {
        db: 'fitbitPerson1AvgCalories',
        coll: 'person1AvgNightCalories',
      },
    },
  },
]);

// Avg Steps in the morning for Person1 in August
db.person1MorningSteps.aggregate([
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
      User1AvgAugMorningSteps: { $avg: { $sum: '$convertedSteps' } },
    },
  },
  {
    $project: {
      _id: 0,
      User1AvgAugMorningSteps: 1,
    },
  },
]);

// $facet stage doesnt allow $merge or $out
// can be used for testing purpose though
// Person1 Morning, Day, Night Steps
db.person1Steps.aggregate([
  {
    $facet: {
      morning: [
        {
          $addFields: {
            date: { $toDate: '$dateTime' },
            month: { $month: { $toDate: '$dateTime' } },
            time: {
              $arrayElemAt: [
                { $split: [{ $trim: { input: '$dateTime' } }, ' '] },
                1,
              ],
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
          $merge: { into: 'person1MorningSteps' },
        },
      ],

      day: [
        {
          $addFields: {
            date: { $toDate: '$dateTime' },
            month: { $month: { $toDate: '$dateTime' } },
            time: {
              $arrayElemAt: [
                { $split: [{ $trim: { input: '$dateTime' } }, ' '] },
                1,
              ],
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
          $limit: 5,
        },
      ],

      night: [
        {
          $addFields: {
            date: { $toDate: '$dateTime' },
            month: { $month: { $toDate: '$dateTime' } },
            time: {
              $arrayElemAt: [
                { $split: [{ $trim: { input: '$dateTime' } }, ' '] },
                1,
              ],
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
          $limit: 5,
        },
      ],
    },
  },
]);
