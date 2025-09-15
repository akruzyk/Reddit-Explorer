import React from "react";

import CategoryCard from "./CategoryCard.jsx";
import CommunityCard from "./CommunityCard.jsx";
import Pagination from "./Pagination.jsx";
import { loadData, loadStats } from "../utils/api";
import { formatNumber } from "../utils/formatters.js";


const RedditExplorer = () => {
    const [communities, setCommunities] = React.useState([]);
    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState(null);
    const [currentTier, setCurrentTier] = React.useState('all');
    const [currentCategory, setCurrentCategory] = React.useState('all');
    const [currentPage, setCurrentPage] = React.useState(1);
    const [searchTerm, setSearchTerm] = React.useState('');
    const [searchMode, setSearchMode] = React.useState('name');
    const [sortBy, setSortBy] = React.useState('subscribers');
    const [nsfwOnly, setNsfwOnly] = React.useState(false);
    const [isCompact, setIsCompact] = React.useState(true);
    const [categoryView, setCategoryView] = React.useState('cards');
    const [pagination, setPagination] = React.useState({});
    const [stats, setStats] = React.useState({
        total: 0,
        total_subscribers: 0,
        avg_subscribers: 0
    });
    const [expandedCards, setExpandedCards] = React.useState(new Set());
    const [categoryCounts, setCategoryCounts] = React.useState({});

    // Function to categorize communities based on their tags
    const getSubredditTags = (community) => {
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
            tags.push({ name: 'technology', color: 'bg-blue-500' });
        }
        if (name.includes('ask') || name.includes('discussion') || description.includes('discuss')) {
            tags.push({ name: 'discussion', color: 'bg-purple-500' });
        }
        if (name.includes('meme') || name.includes('funny') || description.includes('humor')) {
            tags.push({ name: 'humor', color: 'bg-yellow-500' });
        }
        if (name.includes('photo') || name.includes('art') || name.includes('image') || description.includes('picture')) {
            tags.push({ name: 'images', color: 'bg-pink-500' });
        }
        if (name.includes('news') || description.includes('current event')) {
            tags.push({ name: 'news', color: 'bg-orange-500' });
        }
        if (name.includes('creative') || name.includes('art') || description.includes('design')) {
            tags.push({ name: 'creative', color: 'bg-teal-500' });
        }
        if (name.includes('support') || description.includes('help')) {
            tags.push({ name: 'support', color: 'bg-indigo-500' });
        }
        
        return tags;
    };

    // Function to determine the primary category for a community
    const getPrimaryCategory = (community) => {
        const tags = getSubredditTags(community);
        if (tags.length > 0) {
            return tags[0].name;
        }
        return 'other';
    };

    // Update category counts when communities data changes
    React.useEffect(() => {
        if (communities.length > 0) {
            const counts = {
                'all': communities.length,
                'discussion': 0,
                'humor': 0,
                'gaming': 0,
                'technology': 0,
                'images': 0,
                'news': 0,
                'creative': 0,
                'support': 0,
                'nsfw': 0,
                'other': 0
            };
            
            communities.forEach(community => {
                const category = getPrimaryCategory(community);
                if (counts.hasOwnProperty(category)) {
                    counts[category]++;
                } else {
                    counts['other']++;
                }
                
                // Also count NSFW separately
                if (community.over18 || 
                    (community.display_name || '').toLowerCase().includes('nsfw') || 
                    (community.public_description || '').toLowerCase().includes('nsfw')) {
                    counts['nsfw']++;
                }
            });
            
            setCategoryCounts(counts);
        }
    }, [communities]);

    const categories = [
        { id: 'all', name: 'All Communities', icon: '🌐', description: 'Browse all available communities', count: categoryCounts.all || 0 },
        { id: 'discussion', name: 'Discussion & Conversation', icon: '💬', description: 'Communities focused on discussion and conversation', count: categoryCounts.discussion || 0 },
        { id: 'humor', name: 'Humor & Memes', icon: '😂', description: 'Funny content, memes, and comedy', count: categoryCounts.humor || 0 },
        { id: 'gaming', name: 'Gaming', icon: '🎮', description: 'Video games, gaming news, and gaming communities', count: categoryCounts.gaming || 0 },
        { id: 'technology', name: 'Technology', icon: '💻', description: 'Tech news, programming, and technology discussions', count: categoryCounts.technology || 0 },
        { id: 'images', name: 'Images & Photography', icon: '📸', description: 'Photo sharing, art, and visual content', count: categoryCounts.images || 0 },
        { id: 'news', name: 'News & World Events', icon: '📰', description: 'Current events, news, and world happenings', count: categoryCounts.news || 0 },
        { id: 'creative', name: 'Creative & Arts', icon: '🎨', description: 'Art, music, writing, and creative works', count: categoryCounts.creative || 0 },
        { id: 'support', name: 'Support Communities', icon: '🤝', description: 'Support groups and helpful communities', count: categoryCounts.support || 0 },
        { id: 'nsfw', name: 'NSFW Content', icon: '🔞', description: 'Adult content and NSFW communities', count: categoryCounts.nsfw || 0 }
    ];

    const handleTierChange = (tier) => {
        setCurrentTier(tier);
        setCurrentPage(1);
    };

    const handleCategoryChange = (categoryId) => {
        setCurrentCategory(categoryId);
        setCurrentPage(1);
    };

    const handleNsfwToggle = () => {
        setNsfwOnly(!nsfwOnly);
        setCurrentPage(1);
    };

    const handleSearchChange = (value) => {
        setSearchTerm(value);
        setCurrentPage(1);
    };

    const handlePageChange = (page) => {
        setCurrentPage(page);
        const resultsElement = document.querySelector('.divide-y');
        if (resultsElement) {
            resultsElement.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }
    };

    const toggleCardExpansion = (index) => {
        const newExpanded = new Set(expandedCards);
        if (newExpanded.has(index)) {
            newExpanded.delete(index);
        } else {
            newExpanded.add(index);
        }
        setExpandedCards(newExpanded);
    };

    React.useEffect(() => {
        loadData(setCommunities, setPagination, setLoading, setError, { 
            currentTier, 
            searchTerm, 
            searchMode, 
            sortBy, 
            currentPage, 
            nsfwOnly 
        });
    }, [currentTier, searchMode, sortBy, currentPage, nsfwOnly, searchTerm]);

    React.useEffect(() => {
        loadStats(setStats, { currentTier, nsfwOnly });
    }, [currentTier, nsfwOnly]);

    const CategoryCard = ({ category, onClick, isActive }) => (
        React.createElement(
            'div',
            {
                className: `category-card p-5 rounded-xl cursor-pointer transition-all duration-300 ${
                    isActive
                        ? 'bg-gradient-to-br from-indigo-500 to-purple-600 text-white border-2 border-indigo-500'
                        : 'bg-gradient-to-br from-gray-50 to-gray-100 hover:from-gray-100 hover:to-gray-200 border-2 border-transparent hover:border-indigo-500'
                } hover:transform hover:-translate-y-1 hover:shadow-lg`,
                onClick,
                role: 'button',
                tabIndex: 0,
                'aria-label': `Select ${category.name} category`,
                onKeyDown: (e) => e.key === 'Enter' && onClick()
            },
            React.createElement('span', { className: 'text-2xl mb-3 block', 'aria-hidden': true }, category.icon),
            React.createElement('div', { className: 'text-lg font-semibold mb-1' }, category.name),
            React.createElement('div', { className: `text-sm mb-2 ${isActive ? 'opacity-90' : 'opacity-70'}` }, `${formatNumber(category.count)} communities`),
            React.createElement('div', { className: `text-xs leading-tight ${isActive ? 'opacity-80' : 'opacity-60'}` }, category.description)
        )
    );

    

    const Pagination = ({ pagination }) => {
        if (!pagination || pagination.total_pages <= 1) return null;

        const { page, total_pages } = pagination;
        const startPage = Math.max(1, page - 2);
        let endPage = Math.min(total_pages, startPage + 4);

        return React.createElement(
            'div',
            { className: 'flex justify-center gap-1 my-2' },
            page > 1 &&
                React.createElement(
                    'button',
                    {
                        onClick: () => handlePageChange(page - 1),
                        className: 'px-2 py-1 border border-gray-300 rounded-md bg-white hover:bg-gray-50 text-xs'
                    },
                    '←'
                ),
            Array.from({ length: endPage - startPage + 1 }, (_, i) => startPage + i).map(pageNum =>
                React.createElement(
                    'button',
                    {
                        key: pageNum,
                        onClick: () => handlePageChange(pageNum),
                        className: `px-2 py-1 border rounded-md text-xs ${
                            pageNum === page ? 'bg-indigo-600 text-white border-indigo-600' : 'bg-white border-gray-300 hover:bg-gray-50'
                        }`
                    },
                    pageNum
                )
            ),
            page < total_pages &&
                React.createElement(
                    'button',
                    {
                        onClick: () => handlePageChange(page + 1),
                        className: 'px-2 py-1 border border-gray-300 rounded-md bg-white hover:bg-gray-50 text-xs'
                    },
                    '→'
                )
        );
    };

    return React.createElement(
        'div',
        { className: 'min-h-screen bg-gradient-to-br from-indigo-500 to-purple-600' },
        React.createElement(
            'div',
            { className: 'max-w-7xl mx-auto px-5 py-8' },
            React.createElement(
                'div',
                { className: 'text-center mb-10 text-white' },
                React.createElement('h1', { className: 'text-4xl font-bold mb-3 text-shadow-lg' }, '🚀 Reddit Community Explorer'),
                React.createElement('p', { className: 'text-lg opacity-90' }, 'Discover and explore Reddit\'s most popular communities')
            ),
            React.createElement(
                'div',
                { className: 'bg-white bg-opacity-95 rounded-2xl p-6 mb-6 backdrop-blur-sm shadow-xl' },
                React.createElement(
                    'div',
                    { className: 'flex items-center gap-2 p-3 bg-red-50 border-2 border-red-200 rounded-md mb-4' },
                    React.createElement('input', {
                        type: 'checkbox',
                        id: 'nsfwOnly',
                        checked: nsfwOnly,
                        onChange: handleNsfwToggle,
                        className: 'w-4 h-4 text-red-600'
                    }),
                    React.createElement(
                        'label',
                        { htmlFor: 'nsfwOnly', className: 'font-semibold text-red-700 cursor-pointer text-sm' },
                        'Show ONLY NSFW Content'
                    )
                ),
                React.createElement(
                    'div',
                    { className: 'mb-6' },
                    React.createElement(
                        'div',
                        { className: 'flex justify-between items-center mb-4' },
                        React.createElement('h3', { className: 'text-lg font-semibold text-gray-700' }, 'Browse by Category'),
                        React.createElement(
                            'div',
                            { className: 'flex gap-1' },
                            ['cards', 'list'].map(view =>
                                React.createElement(
                                    'button',
                                    {
                                        key: view,
                                        onClick: () => setCategoryView(view),
                                        className: `px-2 py-1 rounded-md text-xs font-medium transition-all ${
                                            categoryView === view
                                                ? 'bg-indigo-600 text-white'
                                                : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
                                        }`
                                    },
                                    view === 'cards' ? '📱 Cards' : '📋 List'
                                )
                            )
                        )
                    ),
                    categoryView === 'cards'
                        ? React.createElement(
                              'div',
                              { className: 'grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5 gap-3' },
                              categories.map(category =>
                                  React.createElement(CategoryCard, {
                                      key: category.id,
                                      category,
                                      onClick: () => handleCategoryChange(category.id),
                                      isActive: currentCategory === category.id
                                  })
                              )
                          )
                        : React.createElement(
                              'div',
                              { className: 'space-y-1' },
                              categories.map(category =>
                                  React.createElement(
                                      'button',
                                      {
                                          key: category.id,
                                          className: `w-full text-left p-2 rounded-md text-sm ${
                                              currentCategory === category.id
                                                  ? 'bg-indigo-600 text-white'
                                                  : 'bg-white text-gray-800'
                                          } hover:bg-indigo-500 hover:text-white`,
                                          onClick: () => handleCategoryChange(category.id)
                                      },
                                      `${category.icon} ${category.name} (${formatNumber(category.count)})`
                                  )
                              )
                          )
                ),
                React.createElement(
                    'div',
                    { className: 'flex gap-2 mb-4 flex-wrap justify-center' },
                    [
                        { id: 'all', name: 'All Communities' },
                        { id: 'major', name: 'Major (1M+)' },
                        { id: 'rising', name: 'Rising (100k-999k)' },
                        { id: 'growing', name: 'Growing (10k-99k)' },
                        { id: 'emerging', name: 'Emerging (1k-9.9k)' }
                    ].map(tier =>
                        React.createElement(
                            'button',
                            {
                                key: tier.id,
                                onClick: () => handleTierChange(tier.id),
                                className: `px-3 py-1 rounded-full font-medium transition-all transform text-sm ${
                                    currentTier === tier.id
                                        ? 'bg-indigo-600 text-white shadow-md scale-105'
                                        : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                                }`
                            },
                            tier.name
                        )
                    )
                ),
                React.createElement(
                    'div',
                    { className: 'grid grid-cols-1 md:grid-cols-3 gap-3 mb-4' },
                    React.createElement(
                        'div',
                        { className: 'relative' },
                        React.createElement('span', { className: 'absolute left-2 top-1/2 transform -translate-y-1/2 text-gray-400 text-xs' }, '🔍'),
                        React.createElement('input', {
                            type: 'text',
                            placeholder: 'Search communities...',
                            value: searchTerm,
                            onChange: (e) => handleSearchChange(e.target.value),
                            className: 'w-full pl-6 pr-2 py-1 border-2 border-gray-200 rounded-md focus:border-indigo-500 focus:outline-none transition-colors text-sm'
                        })
                    ),
                    React.createElement(
                        'select',
                        {
                            value: searchMode,
                            onChange: (e) => setSearchMode(e.target.value),
                            className: 'px-2 py-1 border-2 border-gray-200 rounded-md focus:border-indigo-500 focus:outline-none transition-colors text-sm'
                        },
                        React.createElement('option', { value: 'name' }, 'Name Only'),
                        React.createElement('option', { value: 'description' }, 'Description Only'),
                        React.createElement('option', { value: 'all' }, 'Name & Description')
                    ),
                    React.createElement(
                        'select',
                        {
                            value: sortBy,
                            onChange: (e) => setSortBy(e.target.value),
                            className: 'px-2 py-1 border-2 border-gray-200 rounded-md focus:border-indigo-500 focus:outline-none transition-colors text-sm'
                        },
                        React.createElement('option', { value: 'subscribers' }, 'Subscribers (High to Low)'),
                        React.createElement('option', { value: 'subscribers_asc' }, 'Subscribers (Low to High)'),
                        React.createElement('option', { value: 'name' }, 'Name (A-Z)'),
                        React.createElement('option', { value: 'name_desc' }, 'Name (Z-A)'),
                        React.createElement('option', { value: 'created' }, 'Newest First'),
                        React.createElement('option', { value: 'created_desc' }, 'Oldest First')
                    )
                ),
                React.createElement(
                    'div',
                    { className: 'flex gap-4 justify-center flex-wrap' },
                    [
                        { label: 'Communities Shown', value: formatNumber(stats.total), icon: '🏘️' },
                        { label: 'Total Subscribers', value: formatNumber(stats.total_subscribers), icon: '👥' },
                        { label: 'Avg Subscribers', value: formatNumber(stats.avg_subscribers), icon: '📊' }
                    ].map((stat, idx) =>
                        React.createElement(
                            'div',
                            { key: idx, className: 'text-center bg-white rounded-xl p-2 shadow-md min-w-24' },
                            React.createElement('div', { className: 'text-xl mb-1' }, stat.icon),
                            React.createElement('div', { className: 'text-lg font-bold text-indigo-600' }, stat.value),
                            React.createElement('div', { className: 'text-xs text-gray-600' }, stat.label)
                        )
                    )
                )
            ),
            React.createElement(
                'div',
                { className: 'bg-white bg-opacity-95 rounded-2xl overflow-hidden shadow-xl backdrop-blur-sm' },
                React.createElement(
                    'div',
                    { className: 'bg-indigo-600 text-white p-4' },
                    React.createElement(
                        'div',
                        { className: 'flex justify-between items-center' },
                        React.createElement(
                            'div',
                            null,
                            React.createElement('h2', { className: 'text-xl font-bold' }, 'Communities'),
                            pagination.total &&
                                React.createElement(
                                    'p',
                                    { className: 'opacity-90 mt-1 text-sm' },
                                    `Showing ${(pagination.page - 1) * pagination.per_page + 1}-${Math.min(
                                        pagination.page * pagination.per_page,
                                        pagination.total
                                    )} of ${pagination.total.toLocaleString()} subreddits`
                                )
                        ),
                        React.createElement(
                            'button',
                            {
                                onClick: () => setIsCompact(!isCompact),
                                className: 'px-2 py-1 bg-white bg-opacity-20 hover:bg-opacity-30 rounded-md transition-all font-medium text-xs',
                                title: 'Toggle view mode'
                            },
                            isCompact ? 'Expanded View' : 'Compact View'
                        )
                    )
                ),
                React.createElement(
                    'div',
                    { className: 'divide-y' },
                    loading
                        ? React.createElement(
                              'div',
                              { className: 'text-center py-6 text-indigo-600 text-base' },
                              React.createElement('div', {
                                  className:
                                      'inline-block w-6 h-6 animate-spin rounded-full border-4 border-solid border-current border-r-transparent align-[-0.125em] motion-reduce:animate-[spin_1.5s_linear_infinite] mb-2'
                              }),
                              React.createElement('div', null, 'Loading communities...')
                          )
                        : error
                        ? React.createElement(
                              'div',
                              { className: 'text-center py-6 text-red-600 text-base' },
                              '❌ ',
                              error
                          )
                        : communities.length === 0
                        ? React.createElement(
                              'div',
                              { className: 'text-center py-6 text-gray-600' },
                              React.createElement('h3', { className: 'text-base font-semibold mb-1' }, 'No communities found'),
                              React.createElement('p', null, 'Try adjusting your search or filter criteria')
                          )
                        : communities.map((community, index) =>
                              React.createElement(CommunityCard, {
                                  key: index,
                                  community,
                                  index,
                                  isExpanded: expandedCards.has(index),
                                  onToggle: toggleCardExpansion,
                                  isCompact,
                                  searchTerm: searchTerm
                              })
                          )
                ),
                React.createElement(Pagination, {
                    pagination: pagination
                })
            )
        )
    );
};

export default RedditExplorer;