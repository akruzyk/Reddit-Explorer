import React, { useState, useEffect } from 'react';
import { useLocation, Link } from 'react-router-dom';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend } from 'recharts';

const SubredditStats = () => {
    const [stats, setStats] = useState([]);
    const [loading, setLoading] = useState(true);
    const query = new URLSearchParams(useLocation().search);
    const subreddit = query.get('subreddit') || '';

    useEffect(() => {
        if (subreddit) {
            fetch(`/api/comments/${subreddit}`)
                .then(res => res.json())
                .then(data => {
                    setStats(data.stats || []);
                    setLoading(false);
                })
                .catch(() => setLoading(false));
        }
    }, [subreddit]);

    if (loading) return <div className="text-center text-gray-500 py-6">Loading...</div>;
    if (!subreddit) return <div className="text-center text-gray-500 py-6">No subreddit selected</div>;

    return (
        <div className="container mx-auto p-6 bg-gradient-to-br from-gray-50 to-gray-100 min-h-screen">
            <Link to="/" className="text-indigo-600 hover:underline mb-4 inline-block">&larr; Back to Explorer</Link>
            <h1 className="text-3xl font-bold mb-6 text-gray-800">Stats for r/{subreddit}</h1>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
                {stats.map(stat => (
                    <div key={`${stat.year}-${stat.month}`} className="p-4 border rounded-lg shadow-md bg-white">
                        <h3 className="text-lg font-semibold text-gray-800">{stat.display_name} ({stat.month}/{stat.year}) {stat.legacy ? '(Legacy)' : ''}</h3>
                        <p className="text-sm text-gray-600">Comments: {stat.comment_count.toLocaleString()}</p>
                    </div>
                ))}
            </div>
            <h2 className="text-xl font-semibold mb-4 text-gray-800">Comment Count Over Time</h2>
            <LineChart
                width={600}
                height={300}
                data={stats}
                margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
                className="bg-white p-4 rounded-lg shadow-md"
            >
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey={s => `${s.month}/${s.year}`} />
                <YAxis />
                <Tooltip />
                <Legend />
                <Line type="monotone" dataKey="comment_count" stroke="#4f46e5" strokeWidth={2} />
            </LineChart>
        </div>
    );
};

export default SubredditStats;