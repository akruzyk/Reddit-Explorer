import React, { useState, useEffect } from "react";
import { formatNumber, formatDate, getSubredditTags } from "../utils/formatters";
import { getCommentHistory } from "../utils/api";


const CommunityCard = ({ community, index, isExpanded, onToggle, isCompact, searchTerm }) => {
  const isBlurred = community.over18 && !isExpanded;
  const [commentHistory, setCommentHistory] = useState(null);
  const [loadingComments, setLoadingComments] = useState(false);
  const [showCommentHistory, setShowCommentHistory] = useState(false);
  const tags = getSubredditTags(community);

  const highlightText = (text, term) => {
    if (!term || !text) return text;
    const regex = new RegExp(`(${term})`, "gi");
    return text.replace(
      regex,
      '<span style="background-color: #FFEB3B; padding: 0 2px; border-radius: 2px;">$1</span>'
    );
  };

  useEffect(() => {
    // Load comment history when expanded and showCommentHistory is true
    if (isExpanded && showCommentHistory && !commentHistory) {
      setLoadingComments(true);
      getCommentHistory(community.display_name)
        .then((data) => {
          setCommentHistory(data);
          setLoadingComments(false);
        })
        .catch((err) => {
          console.error(err);
          setCommentHistory([]);
          setLoadingComments(false);
        });
    }
  }, [isExpanded, showCommentHistory, community.display_name]);

  const handleToggle = () => {
    onToggle(index);
  };

  const toggleCommentHistory = (e) => {
    e.stopPropagation();
    setShowCommentHistory(!showCommentHistory);
  };

  return (
    <div
      className={`community-card p-4 rounded-lg shadow-lg bg-white ${
        isCompact ? "max-w-full" : "w-full"
      } transition-all duration-300 cursor-pointer border border-gray-200 hover:border-indigo-500 hover:bg-gray-50`}
      onClick={handleToggle}
    >
      {/* Header */}
      <div className="flex items-start justify-between w-full items-start mb-2">
        <a
          href={`https://reddit.com/r/${community.display_name}`}
          target="_blank"
          rel="noopener noreferrer"
          className={`community-link font-semibold ${
            isCompact ? "text-sm" : "text-lg"
          } text-indigo-600 hover:text-indigo-800 hover:underline ${isBlurred ? "blur-sm filter" : ""}`}
          onClick={(e) => e.stopPropagation()}
          dangerouslySetInnerHTML={{ __html: highlightText(community.display_name, searchTerm) }}
        />
        <button
          className="text-gray-400 hover:text-indigo-600 transition-colors p-1"
          onClick={(e) => {
            e.stopPropagation();
            handleToggle();
          }}
        >
          {isExpanded ? "▲" : "▼"}
        </button>
      </div>


      {/* Stats and Tags */}
      <div className="flex flex-wrap items-center gap-2 text-sm text-gray-600">
        <span className="bg-blue-100 text-blue-800 px-2 py-1 rounded-full font-medium">
          {formatNumber(community.subscribers)}
        </span>
        <span className="text-gray-500" title="Created Date">
          📅 {formatDate(community.created_date)}
        </span>
        {tags.map((tag) => (
          <span
            key={tag.name}
            className={`inline-block ${tag.color} text-white px-2 py-1 rounded text-xs font-medium`}
          >
            {tag.name.toUpperCase()}
          </span>
        ))}
      </div>

      {/* Expanded Details */}
      {isExpanded && (
        <div className="mt-4 pt-4 border-t border-gray-200 text-gray-700 leading-relaxed">
          <div
            className={`text-${isCompact ? "sm" : "lg"} mb-3`}
            dangerouslySetInnerHTML={{
              __html: highlightText(community.public_description || "No description available", searchTerm),
            }}
          />
          <div className={`mt-4 text-${isCompact ? "xs" : "base"} font-medium mb-4`}>
            <p>Category: {community.category || "None"}</p>
            <p>Growth: {community.growth || 0}</p>
            <p>Activity: {community.activity || 0}</p>
            {community.over18 && <p className="text-red-600 font-medium">NSFW Content</p>}

          {/* Comment History Toggle */}
          <button
            onClick={toggleCommentHistory}
            className="text-sm text-blue-500 hover:text-blue-700 mb-2 flex items-center"
          >
            {showCommentHistory ? 'Hide Comment History' : 'Show Comment History'}
            <svg 
              className={`ml-1 h-4 w-4 transition-transform ${showCommentHistory ? 'rotate-180' : ''}`}
              fill="none" 
              stroke="currentColor" 
              viewBox="0 0 24 24"
            >
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
            </svg>
          </button>
                    </div>


          {/* Comment History Display */}
          {showCommentHistory && (
            <div className="mt-2 p-3 bg-gray-50 rounded-lg">
              <h4 className="text-sm font-medium mb-2">Monthly Comment Counts</h4>
              {loadingComments ? (
                <div className="flex justify-center items-center h-20">
                  <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-500"></div>
                </div>
              ) : commentHistory && commentHistory.length > 0 ? (
                <div className="max-h-40 overflow-y-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b">
                        <th className="text-left py-1">Period</th>
                        <th className="text-right py-1">Comments</th>
                      </tr>
                    </thead>
                    <tbody>
                      {commentHistory.map((item, idx) => (
                        <tr key={idx} className="border-b border-gray-100">
                          <td className="py-1">{item.month}</td>
                          <td className="text-right py-1">{formatNumber(item.count)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <p className="text-gray-500 text-sm">No comment data available.</p>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default CommunityCard;

