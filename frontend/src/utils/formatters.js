export const formatNumber = (num) => {
    if (!num) return '0';
    if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
    if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
    return num.toString();
};

export const formatDate = (dateString) => {
    if (!dateString) return 'Unknown date';
    try {
        const date = new Date(dateString);
        return date.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
    } catch (e) {
        return dateString;
    }
};

export const getYearsAgo = (dateString) => {
    if (!dateString) return 0;
    try {
        const date = new Date(dateString);
        const now = new Date();
        const diffTime = Math.abs(now - date);
        return Math.floor(diffTime / (1000 * 60 * 60 * 24 * 365.25));
    } catch (e) {
        return 0;
    }
};

export const getSubredditTags = (community) => {
    const tags = [];
    const name = (community.display_name || '').toLowerCase();
    const description = (community.public_description || community.description || '').toLowerCase();
    
    if (community.over18 || name.includes('nsfw') || description.includes('nsfw')) {
        tags.push({ name: 'nsfw', color: 'bg-red-500' });
    }
    if (name.includes('gaming') || name.includes('game') || description.includes('game')) {
        tags.push({ name: 'gaming', color: 'bg-green-500' });
    }
    if (name.includes('tech') || name.includes('programming') || name.includes('code') || description.includes('technology')) {
        tags.push({ name: 'tech', color: 'bg-blue-500' });
    }
    if (name.includes('ask') || name.includes('discussion') || description.includes('discuss')) {
        tags.push({ name: 'discussion', color: 'bg-purple-500' });
    }
    
    return tags;
};
