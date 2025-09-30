const API_BASE = 'http://localhost:5001';

export const loadData = async (setCommunities, setPagination, setLoading, setError, { currentTier, searchTerm, searchMode, sortBy, currentPage, nsfwOnly }) => {
    setLoading(true);
    try {
        const params = new URLSearchParams({
            tier: currentTier,
            sort: sortBy,
            page: currentPage,
            per_page: 50,
            nsfw: nsfwOnly ? 'true' : 'false'
        });
        if (searchTerm.trim()) {
            params.append('search', searchTerm);
            params.append('mode', searchMode);
        }
        console.log(`Fetching communities: /api/communities?${params}`);
        const response = await fetch(`/api/communities?${params}`);
        if (!response.ok) {
            throw new Error(`HTTP error: ${response.status}`);
        }
        const data = await response.json();
        setCommunities(Array.isArray(data.data) ? data.data : []);
        setPagination(data.pagination || {});
        setLoading(false);
    } catch (err) {
        console.error('Fetch error:', err);
        setError(err.message);
        setCommunities([]);
        setLoading(false);
    }
};

export const loadStats = async (setStats, { currentTier, nsfwOnly }) => {
    try {
        const params = new URLSearchParams({
            tier: currentTier,
            nsfw: nsfwOnly ? 'true' : 'false'
        });
        console.log(`Fetching stats: /api/stats?${params}`);
        const response = await fetch(`/api/stats?${params}`);
        if (!response.ok) {
            throw new Error(`HTTP error: ${response.status}`);
        }
        const data = await response.json();
        setStats(data);
    } catch (err) {
        console.error('Stats fetch error:', err);
    }
};

export const getCommentHistory = async (subreddit) => {
  try {
    const response = await fetch(`${API_BASE}/api/comments/${subreddit}`);
    if (!response.ok) throw new Error("Failed to fetch comment history");
    const data = await response.json();
    return data.data; // <-- Make sure you return the array!
  } catch (error) {
    console.error(error);
    return [];
  }
};

